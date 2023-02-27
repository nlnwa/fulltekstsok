import math

import psycopg2 as pg
import streamlit as st

import config as c


def create_link(link, link_text):
    return f"[{link_text}]({link})"


def create_link_html(link, link_text):
    return f"<a href=\"{link}\">{link_text}</a>"


def get_docs_websearch(query="'Nettarkivet'", limit=10, window=25, samplesize=10):
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

        if nr_docs > limit:
            cur.execute("""CREATE TEMP TABLE query_results_limit AS
                SELECT * FROM query_results q
                TABLESAMPLE BERNOULLI (%s)
                LIMIT %s;""", (samplesize, limit,))

            circa_nr = math.floor(nr_docs * (samplesize / 100))

        else:
            cur.execute("""CREATE TEMP TABLE query_results_limit AS
                SELECT * FROM query_results q;""")

            circa_nr = None

        # analyze temp table
        cur.execute("ANALYZE query_results_limit;")
        sql = f"""SELECT ts_headline('norwegian', (array_agg(fulltext))[1], websearch_to_tsquery('norwegian', %s), 'MaxFragments=1, MaxWords=%s, MinWords=5') as conc, (array_agg(substring(wf.date, 1, 10)))[1] as date, (array_agg('{c.wayback_url}' || regexp_replace(substring(wf.date, 1, 19), '\D', '', 'g') || '/' || wf.target_uri))[1] as loke_url, (array_agg(wf.target_uri))[1] as web_url, ft.fulltext_hash
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
    if circa_nr == None:
        st.markdown("Totalt " + str(nr_docs) + " treff (fullstendig), viser " + str(nr_showing))
    elif circa_nr != None:
        st.markdown(
            "Totalt " + str(nr_docs) + " treff (samplet " + str(circa_nr) + " av disse), viser " + str(nr_showing))
    for idx, result in enumerate(results):
        link = create_link(link=result[2], link_text=result[3])
        st.markdown("_" + result[1] + " -- " + link + "_")

        if debug == True:
            st.markdown(result[0] + " (" + result[4] + ")", unsafe_allow_html=True)
        else:
            st.markdown(result[0], unsafe_allow_html=True)


# Streamlit stuff
st.set_page_config(initial_sidebar_state="expanded", layout="wide")
st.title('Fulltekstsøk i Nettarkivet')

st.info("""Oppgi et søkeord eller søkeuttrykk. For å søke etter eksakte fraser plasserer du ordene innenfor "sitattegn". Ved svært mange treff vil resultatet randomiseres, uten sortering på relevans. Duplikater kan forekomme.""")

query = st.text_input("Søk", "\"Nettarkivet\"",
                      help="Mellomrom mellom ord og fraser fungerer som AND. Dette vil begrense resultatet og returnere treff som oppfyller alle søkeverdiene. Du kan også bruke OR som logisk operator mellom ord/fraser - dette returnerer treff i dokumenter der minimum ett av ordene/frasene forekommer.")

samplesize = st.sidebar.number_input('Sample-størrelse (i %)', value=10,
                                     help="Ved svært mange treff vil søketjenesten gjøre et tilfeldig utvalg av det totale antallet tekster.")
limit = st.sidebar.number_input('Maksimalt antall treff fra sample som vises', value=10,
                                help="Verdien angir antall treff som returneres i søkeresultatet. Flere dokumenter gir lengre søketid.")
window = st.sidebar.number_input('Antall ord per treff (utdrag)', value=25, max_value=50, min_value=6,
                                 help="Angir hvor mange ord som vises i søkeresultatets utdrag. Maksimal verdi er 50.")
debug = st.sidebar.checkbox("Debug", value=False, help="Intern funksjon for å identifisere mulige duplikater")

if window > 50:
    window = 50

nr_docs, nr_showing, circa_nr, results = get_docs_websearch(query=query, limit=limit, window=window,
                                                            samplesize=samplesize)

if nr_docs > nr_showing:
    if st.button("Utfør nytt søk",
                 help="""Prototypen tilbyr dessverre ikke visning av flere sider, slik som i Google. Om du ønsker flere resultater kan du øke verdien for "Maksimalt antall treff"."""):
        nr_docs, nr_showing, circa_nr, results = get_docs_websearch(query=query, limit=limit, window=window,
                                                                    samplesize=samplesize)

print_results(nr_docs, nr_showing, circa_nr, results, debug)
