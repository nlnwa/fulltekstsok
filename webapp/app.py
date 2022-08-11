import streamlit as st
import psycopg2 as pg
import pandas as pd
import datetime
import math
import base64
import json
from io import BytesIO
from random import sample
import config as c

def create_link(link, link_text):
    return f"[{link_text}]({link})"

def create_link_html(link, link_text):
    return f"<a href=\"{link}\">{link_text}</a>"

def get_docs_websearch(query="'Aslak Sira Myhre'", limit=10, window=25, samplesize=10):
    with pg.connect(user=c.user, password=c.password, database=c.database, host=c.host, port=c.port) as con:
        cur = con.cursor()

        # set parallel params
        cur.execute("SET max_parallel_workers = 12;")
        cur.execute("SET max_parallel_workers_per_gather = 12;")
        cur.execute("SET parallel_leader_participation = off;")
        cur.execute("SET effective_io_concurrency = 4;")

        cur.execute("SET parallel_tuple_cost = 0;")
        cur.execute("SET parallel_setup_cost = 0;")
        cur.execute("SET min_parallel_table_scan_size = 0;")
        cur.execute("SET min_parallel_index_scan_size =0;")

        cur.execute("set enable_partitionwise_join = 'on';")

        cur.execute("""CREATE TEMP TABLE query_results AS
                        SELECT ft.fulltext_hash, ft.crawl_id
                        FROM fulltext ft
                        WHERE fulltext_fts @@ websearch_to_tsquery('norwegian', %s);""", (query,))

        cur.execute("SELECT count(*) FROM query_results;")

        nr_docs = cur.fetchone()[0]

        if nr_docs > (limit * 10): 
            cur.execute("""CREATE TEMP TABLE query_results_limit AS
                SELECT * FROM query_results q
                TABLESAMPLE BERNOULLI (%s)
                LIMIT %s;""", (samplesize, limit,))

            circa_nr = math.floor(nr_docs / 10) * 10
        else:
            cur.execute("""CREATE TEMP TABLE query_results_limit AS
                SELECT * FROM query_results q;""")

            circa_nr = nr_docs

        # analyze temp table
        cur.execute("ANALYZE query_results_limit;")
        
        sql = """SELECT ts_headline('norwegian', (array_agg(fulltext))[1], websearch_to_tsquery('norwegian', %s), 'MaxFragments=1, MaxWords=%s, MinWords=5') as conc, (array_agg(substring(wf.date, 1, 10)))[1] as date, (array_agg('https://k8s.nb.no/loke/' || regexp_replace(substring(wf.date, 1, 19), '\D', '', 'g') || '/' || wf.target_uri))[1] as loke_url, (array_agg(wf.target_uri))[1] as web_url, ft.fulltext_hash
            FROM
            warcinfo wf 
            JOIN query_results_limit ft ON ft.fulltext_hash = wf.fulltext_hash AND ft.crawl_id = wf.crawl_id
            JOIN fulltext ft2 ON ft2.fulltext_hash = ft.fulltext_hash AND ft2.crawl_id = ft.crawl_id
            GROUP BY ft.fulltext_hash
            ORDER BY date DESC;"""

        cur.execute(sql, (query, window))
        results = cur.fetchall()
        nr_showing = len(results)

        return nr_docs, nr_showing, circa_nr, results

def print_results(nr_docs, nr_showing, circa_nr, results, debug):
    st.markdown("Omtrent " + str(circa_nr) + " treff, viser " + str(nr_showing))
    for idx,result in enumerate(results):
        link = create_link(link=result[2], link_text=result[3])
        st.markdown("_" + result[1] + " -- " + link + "_")

        if debug == True:
            st.markdown(result[0] + " (" + result[4] + ")", unsafe_allow_html=True)
        else:
            st.markdown(result[0], unsafe_allow_html=True)

# Streamlit stuff
st.set_page_config(initial_sidebar_state="collapsed", layout="wide")
st.title('Fulltekstsøk i NB Nettarkivet')

st.write("Oppgi et søkeord eller søkeuttrykk. Søk på samme måte som i Google. Det søkes i nettsider høstet etter 2019. Duplikater vil kunne forekomme og ikke alle søkeresultater vil være like relevante.")

query = st.text_input("Søk", "\"Nasjonal bibliotekstrategi\"")
samplesize = st.sidebar.number_input('Sample-størrelse (i %)', value=10)
limit = st.sidebar.number_input('Maksimalt antall treff fra sample som vises', value=10)
window = st.sidebar.number_input('Antall ord per treff (snippet)', value=25, max_value=50, min_value=6)
debug = st.sidebar.checkbox("Debug", value=False)

if window > 50:
    window = 50

nr_docs, nr_showing, circa_nr, results = get_docs_websearch(query=query, limit=limit, window=window, samplesize=samplesize)

if nr_docs > nr_showing:
    if st.button("Vis flere dokumenter"):
        nr_docs, nr_showing, circa_nr, results = get_docs_websearch(query=query, limit=limit, window=window, samplesize=samplesize)

print_results(nr_docs, nr_showing, circa_nr, results, debug)

