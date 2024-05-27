from fastapi import FastAPI, Request
import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel
from typing import Optional
import json

app = FastAPI()

# Database connection details
DB_HOST = 'localhost'
DB_PORT = 5434
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'araspah123'

# Connect to the database
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    cursor_factory=RealDictCursor
)

# Create a cursor object
cur = conn.cursor()
print("Database connection open")

class PartnerRequest(BaseModel):
    partner_id: int


compliance_config = {
    10: [
        {'funcCodes': "'1001'", 'lafCategory': 'protected_forest', 'landUseType': 'Protected Forest'},
        {'funcCodes': "'1003'", 'lafCategory': 'production_forest', 'landUseType': 'Fixed Production Forest'},
        {'funcCodes': "'1004'", 'lafCategory': 'production_forest', 'landUseType': 'Limited Production Forest'},
        {'funcCodes': "'1004'", 'lafCategory': 'production_forest', 'landUseType': 'Conversion Production Forest'},
        {'funcCodes': "'1004'", 'lafCategory': 'production_forest', 'landUseType': 'Conservation Forest'},
    ],
    58: [
        {'funcCodes': "'1004'", 'lafCategory': 'production_forest', 'landUseType': 'Conservation Forest'},
        {'funcCodes': "'1004'", 'lafCategory': 'production_forest', 'landUseType': 'Protected Forest'},
    ],
}


def get_country_code(country_id):
    country_codes = {
        10: 'ind',
        58: 'tha',
    }
    return country_codes.get(country_id, '')


def store_compliance(results_redshift, partner_id):
    try:
        for row_redshift in results_redshift:
            cur.execute(
                "INSERT INTO gis_int_eudr_compliance (supplier_id, supplier_display_id, farmnr, commo_id, revision, country_id, province_id, district_id, geom_polygon, partner_id, total_area, def_stat, laf_stat, row_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (row_redshift['supplier_id'], row_redshift['supplier_display_id'], row_redshift['farmnr'], row_redshift['commo_id'], row_redshift['revision'], row_redshift['country_id'], row_redshift['province_id'], row_redshift['district_id'], row_redshift['polygeom'], row_redshift['partner_id'], row_redshift['total_area'], 'compliant', 'compliant', row_redshift['row_id'])
            )
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()


def get_intersect_results(table_name, func_codes, partner_id, country_id):
    query = f"""
        SELECT
            gis_int_eudr_compliance.id,
            gis_int_eudr_compliance.supplier_id,
            st_intersection(ST_MakeValid(ST_SetSRID(gis_int_eudr_compliance.geom_polygon, 4326)), {table_name}.the_geom) as geom_intersect,
            ST_Area(st_intersection(ST_MakeValid(ST_SetSRID(gis_int_eudr_compliance.geom_polygon, 4326)), {table_name}.the_geom)::geography) / 10000 :: numeric AS laf_area,
            ST_Area(gis_int_eudr_compliance.geom_polygon::geography) / 10000 :: numeric AS total_area,
            round(ST_Area(st_intersection(ST_MakeValid(ST_SetSRID(gis_int_eudr_compliance.geom_polygon, 4326)), {table_name}.the_geom)) :: numeric / ST_Area(ST_MakeValid(ST_SetSRID(gis_int_eudr_compliance.geom_polygon, 4326))) ::numeric * 100, 4) AS laf_percent
        FROM gis_int_eudr_compliance
        JOIN (
            SELECT the_geom
            FROM {table_name}
            WHERE gis_id LIKE '0{country_id}%' AND func_code IN ({func_codes})
        ) AS {table_name}
        ON st_intersects(ST_SetSRID(gis_int_eudr_compliance.geom_polygon, 4326), {table_name}.the_geom)
        WHERE gis_int_eudr_compliance.partner_id = %s AND gis_int_eudr_compliance.is_processed = 0;
    """
    cur.execute(query, (partner_id,))
    return cur.fetchall()


def process_country_compliance(partner_id, country_id):
    geojson = {}
    cur.execute("SELECT * FROM gis_int_eudr_compliance WHERE partner_id = %s AND country_id = %s AND is_processed = 0", (partner_id, country_id))
    country_compliance = cur.fetchall()

    for row in country_compliance:
        country_code = get_country_code(country_id)
        geojson.setdefault(partner_id, {}).setdefault(country_id, {'country': []}).get('country').append(country_code)

        for config in compliance_config[country_id]:
            results = get_intersect_results('gis_int_idn_klhk_fkh2019', config['funcCodes'], partner_id, country_id)
            count = len(results)
            land_use_type = config['landUseType'].lower().replace(' ', '_')
            geojson[partner_id][country_id].setdefault(land_use_type, []).append(count)

    return geojson


# @app.post("/eudr/intersect_partner")
# async def intersect_partner(request: Request, partner_request: PartnerRequest):
#     geojson = {}
#     partner_id = partner_request.partner_id

#     cur.execute("SELECT * FROM ktv_dash_eudr_summ_dtl_p0g WHERE partner_id = %s", (partner_id,))
#     results_redshift = cur.fetchall()

#     if results_redshift:
#         store_compliance(results_redshift, partner_id)

#         geojson[partner_id] = {'data_redshift': [len(results_redshift)]}

#         cur.execute("SELECT partner_name FROM gis_int_eudr_catalog WHERE partner_id = %s", (partner_id,))
#         partner_name = cur.fetchone()['partner_name'] if cur.rowcount > 0 else ''
#         geojson[partner_id]['partner_name'] = [partner_name]

#         cur.execute("SELECT DISTINCT country_id FROM gis_int_eudr_compliance WHERE partner_id = %s AND is_processed = 0", (partner_id,))
#         country_ids = [row['country_id'] for row in cur.fetchall()]

#         for country_id in country_ids:
#             geojson.update(process_country_compliance(partner_id, country_id))
#     else:
#         geojson[partner_id] = {'data_redshift': [0], 'partner_name': ['']}

#     cur.execute("UPDATE gis_int_eudr_compliance SET is_processed = 1 WHERE partner_id = %s AND is_processed = 0", (partner_id,))
#     conn.commit()

#     return geojson

@app.post("/eudr/intersect_partner")
async def intersect_partner(request: Request, partner_request: PartnerRequest):
    geojson = {}
    partner_id = partner_request.partner_id

    cur.execute("SELECT * FROM ktv_dash_eudr_summ_dtl_p0g WHERE partner_id = %s", (partner_id,))
    results_redshift = cur.fetchall()

    if results_redshift:
        store_compliance(results_redshift, partner_id)

        geojson[partner_id] = {'data_redshift': [len(results_redshift)]}  # Check for empty results_redshift

        cur.execute("SELECT partner_name FROM gis_int_eudr_catalog WHERE partner_id = %s", (partner_id,))
        partner_name = cur.fetchone()['partner_name'] if cur.rowcount > 0 else ''
        geojson[partner_id]['partner_name'] = [partner_name]

        cur.execute("SELECT DISTINCT country_id FROM gis_int_eudr_compliance WHERE partner_id = %s AND is_processed = 0", (partner_id,))
        country_ids = [row['country_id'] for row in cur.fetchall()]

        for country_id in country_ids:
            try:
                geojson.update(process_country_compliance(partner_id, country_id))
            except IndexError:
                # Handle the case where compliance_config[country_id] might be empty
                pass  # Or log an error message

    else:
        geojson[partner_id] = {'data_redshift': [0], 'partner_name': ['']}

    cur.execute("UPDATE gis_int_eudr_compliance SET is_processed = 1 WHERE partner_id = %s AND is_processed = 0", (partner_id,))
    conn.commit()

    return geojson


@app.post("/eudr/catalog")
async def get_catalog(request: Request, eudr_request: PartnerRequest):
    partner_id = eudr_request.partner_id
    sql = f"SELECT * FROM gis_int_eudr_catalog WHERE partner_id = {partner_id}"
    cur.execute(sql)
    results = cur.fetchall()
    if results:
        return {"data": results}
    else:
        return {"error": "No Data"}

@app.get("/eudr/all_catalog")
async def get_all_catalog(request: Request, limit: int = 0):
    limit_clause = f" LIMIT {limit}" if limit > 0 else ""
    sql = f"SELECT DISTINCT(partner_id) FROM ktv_dash_eudr_summ_dtl_p0g{limit_clause}"
    cur.execute(sql)
    results = cur.fetchall()
    if results:
        return {"data": results}
    else:
        return {"error": "No Data"}

# Close the database connection when the application is stopped
@app.on_event("shutdown")
def shutdown_event():
    cur.close()
    conn.close()
    print("Database connection closed")
