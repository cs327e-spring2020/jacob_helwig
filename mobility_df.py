import datetime, logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class format_alphaCodeFn(beam.DoFn):
  def process(self, element):  
    cov_record = element
    
    if (cov_reco)
    
    code = cov_record.get('country_region_code')
    country = cov_record.get('country_region')
    date = cov_record.get('date')
    rr_ch = cov_record.get('retail_and_recreation_percent_change_from_baseline')
    gp_ch = cov_record.get('grocery_and_pharmacy_percent_change_from_baseline')
    p_ch = cov_record.get('parks_percent_change_from_baseline')	
    t_ch = cov_record.get('transit_stations_percent_change_from_baseline')
    w_ch = cov_record.get('workplaces_percent_change_from_baseline')
    r_ch = cov_record.get('residential_percent_change_from_baseline')	
    
    
    delta = [rr_ch, gp_ch, p_ch, t_ch, w_ch, r_ch]
    delta = [x for x in delta if x is not None]
    if len(delta) == 0:
      return
    avg_chg = int(round(sum(delta)/len(delta),0))
    
    if code == 'GR': # Greece
        code = 'EL'
    if code == 'GB': # United Kingdom
        code = 'UK'
    if code == 'RE': # Reunion
        return
    if code == 'HK': # Hong Kong
        code = 'CN'
    
    return [{'code':code, 'country':country, 'date':date, 'average_change':avg_chg, 'retail_and_recreation':rr_ch, 'grocery_and_pharmacy':gp_ch, 'parks':p_ch, 'transit_stations':t_ch, 'workplaces':w_ch,'residential':r_ch}]

def run():         
    PROJECT_ID = 'corvid-276516'
    BUCKET = 'gs://covid-bucket19' # change to your bucket name
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # Create and set your PipelineOptions.
    options = PipelineOptions(flags=None)

    # For Dataflow execution, set the project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = 'format-codes--df'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)
    
    sql = 'SELECT * FROM covid_staging.googleMobility ORDER BY date, country_region' # passing a query. Shouldn't process more than 1000 records w DR
   
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True) # direct runner is not running in parallel on several workers. DR is local

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source) # read results and assign them to a new p-collection

     # call pardo, pipe query results to pardo
    format_alphaCode_pcoll = query_results | 'Change the country code for Greece, the UK, and Hong Kong. Drop Reunion' >> beam.ParDo(format_alphaCodeFn()) 

     # write PCollection to log file
    format_alphaCode_pcoll | 'Write log 1' >> WriteToText('geodist_beam.txt') 

    dataset_id = 'covid_modeled'
    table_id = 'mobility_beam'
    schema_id = 'code:STRING, country:STRING, date:DATE, average_change:INTEGER, retail_and_recreation:INTEGER, grocery_and_pharmacy:INTEGER, parks:INTEGER, transit_stations:INTEGER, workplaces:INTEGER,residential:INTEGER'

     # write PCollection to new BQ table
    format_alphaCode_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                table=table_id, 
                                                schema=schema_id, 
                                                project=PROJECT_ID,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE, 
                                                batch_size=int(100))
     
    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()