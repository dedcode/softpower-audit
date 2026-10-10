"""Add linked-article provenance without rewriting extraction data."""
from google.cloud import bigquery
FIELDS=('text_origin','publication_url','publication_raw_uri','identity_match')
def apply(client):
 table=client.get_table('citygraph.softpower_crawl.crawl_result_events')
 known={field.name for field in table.schema};missing=[bigquery.SchemaField(name,'STRING') for name in FIELDS if name not in known]
 if missing:
  table.schema=[*table.schema,*missing];client.update_table(table,['schema'])
  view=client.get_table('citygraph.softpower_crawl.crawl_results')
  client.update_table(view,['view_query'])
 return [field.name for field in missing]
if __name__=='__main__':
 import argparse,json
 parser=argparse.ArgumentParser();parser.add_argument('--apply',action='store_true');args=parser.parse_args()
 print(json.dumps({'added':apply(bigquery.Client(project='citygraph'))} if args.apply else {'planned_columns':FIELDS}))
