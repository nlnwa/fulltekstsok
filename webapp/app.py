import streamlit as st
import psycopg2 as pg
import pandas as pd
import datetime
import base64
import json
from io import BytesIO
from random import sample
import config as c

def create_link(link, link_text):
    return f"[{link_text}]({link})"

def create_link_html(link, link_text):
    return f"<a href=\"{link}\">{link_text}</a>"

def get_docs_websearch(query="'Aslak Sira Myhre'", limit=10):
    with pg.connect(user=c.user, password=c.password, database=c.database, host=c.host, port=c.port) as con:
        cur = con.cursor()

        sql = """SELECT ts_headline('norwegian', (array_agg(fulltext))[1], websearch_to_tsquery('norwegian', %s), 'MaxFragments=1, MaxWords=25, MinWords=5'), count(*) as results, (array_agg(substring(wf.date, 1, 10)))[1] as date, (array_agg('https://k8s.nb.no/loke/' || regexp_replace(substring(wf.date, 1, 19), '\D', '', 'g') || '/' || wf.target_uri))[1] as loke_url, (array_agg(wf.target_uri))[1] as web_url
            FROM
            (
                SELECT ft.fulltext_hash, ft.fulltext, ft.crawl_id, ts_rank_cd(fulltext_fts, websearch_to_tsquery('norwegian', %s)) AS rank
                FROM fulltext_reduced ft
                WHERE fulltext_fts @@ websearch_to_tsquery('norwegian', %s)
                ORDER BY rank
                LIMIT %s
            ) x
            JOIN warcinfo wf ON wf.fulltext_hash = x.fulltext_hash AND wf.crawl_id = x.crawl_id
            GROUP BY x.fulltext_hash
            ORDER BY date DESC;"""

        cur.execute(sql, (query,query,query,limit))
        results = cur.fetchall()

        st.markdown("Ca. " + str(len(results)) + " treff.")

        for idx,result in enumerate(results):
            link = create_link(link=result[3], link_text=result[4])
            st.markdown("_" + result[2] + " -- " + link + "_")
            st.markdown(result[0], unsafe_allow_html=True)

# Streamlit stuff
st.set_page_config(initial_sidebar_state="collapsed", layout="wide")
st.title('Fulltekstsøk i NB Nettarkivet')

st.write("Oppgi et søkeord eller søkeuttrykk. Søk på samme måte som i Google. Det søkes i alt av kommunale og statlige nettsider høstet etter 2019. Duplikater vil forekomme og ikke alle søkeresultater vil være like relevante. Dette er en tidlig tilnærming.")

query = st.text_input("Query", "\"Nasjonal bibliotekstrategi\"")
limit = st.sidebar.number_input('Limit', value=100)

get_docs_websearch(query=query, limit=limit)
