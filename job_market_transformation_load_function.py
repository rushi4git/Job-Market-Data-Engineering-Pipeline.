import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

# ===============================
# TRANSFORMATION FUNCTIONS
# ===============================

def unify_company_names(df):
    company_display = df.get('company.display_name')
    company_name = df.get('company_name')

    if company_display is not None and company_name is not None:
        df['company'] = company_display.combine_first(company_name)
    elif company_display is not None:
        df['company'] = company_display
    elif company_name is not None:
        df['company'] = company_name
    else:
        df['company'] = ''
    
    return df

def extract_job_title_counts(adzuna_jobs, arbeitnow_jobs):
    all_jobs = adzuna_jobs + arbeitnow_jobs
    titles = [job.get('title', '').strip() for job in all_jobs if job.get('title')]
    df_titles = pd.DataFrame(titles, columns=['title'])
    title_counts = df_titles['title'].value_counts().reset_index()
    title_counts.columns = ['title', 'count']
    return title_counts

def count_job_postings_per_day(adzuna_jobs, arbeitnow_jobs):
    adzuna_dates = [job['created'][:10] for job in adzuna_jobs if 'created' in job]
    arbeitnow_dates = [job['date_posted'][:10] for job in arbeitnow_jobs if 'date_posted' in job]
    all_dates = adzuna_dates + arbeitnow_dates
    df_dates = pd.DataFrame(all_dates, columns=['created_date'])
    df_dates['created_date'] = pd.to_datetime(df_dates['created_date'])
    date_counts = df_dates['created_date'].value_counts().sort_index().reset_index()
    date_counts.columns = ['date', 'job_postings']
    return date_counts

def count_job_postings_per_month(adzuna_jobs, arbeitnow_jobs):
    adzuna_dates = [job['created'] for job in adzuna_jobs if 'created' in job]
    arbeitnow_dates = [job['date_posted'] for job in arbeitnow_jobs if 'date_posted' in job]
    all_dates = adzuna_dates + arbeitnow_dates
    df = pd.DataFrame(all_dates, columns=['created'])
    df['created'] = pd.to_datetime(df['created'])
    df['posted_month'] = df['created'].dt.to_period('M')
    df_monthly = df['posted_month'].value_counts().sort_index().reset_index()
    df_monthly.columns = ['Month', 'Job_Postings']
    df_monthly['Month'] = df_monthly['Month'].astype(str)
    return df_monthly

def top_hiring_companies(adzuna_jobs, arbeitnow_jobs):
    adzuna_companies = [job.get('company', {}).get('display_name', '') for job in adzuna_jobs]
    arbeitnow_companies = [job.get('company_name', '') for job in arbeitnow_jobs]
    all_companies = adzuna_companies + arbeitnow_companies
    df_companies = pd.DataFrame(all_companies, columns=['company'])
    df_companies = df_companies[df_companies['company'] != '']
    df_company_counts = df_companies['company'].value_counts().reset_index()
    df_company_counts.columns = ['Company', 'Job_Postings']
    return df_company_counts

def location_analysis(adzuna_jobs, arbeitnow_jobs):
    adzuna_locations = [job.get('location', {}).get('display_name', '') for job in adzuna_jobs]
    arbeitnow_locations = [job.get('location', '') for job in arbeitnow_jobs]
    all_locations = adzuna_locations + arbeitnow_locations
    df_locations = pd.DataFrame(all_locations, columns=['location'])
    df_locations = df_locations[df_locations['location'] != '']
    location_counts = df_locations['location'].value_counts().reset_index()
    location_counts.columns = ['location', 'count']
    return location_counts

def contract_type_distribution(adzuna_jobs, arbeitnow_jobs):
    adzuna_contracts = [job.get('contract_time', '') for job in adzuna_jobs]
    arbeitnow_contracts = [job.get('job_types', [''])[0] if job.get('job_types') else '' for job in arbeitnow_jobs]
    all_contracts = adzuna_contracts + arbeitnow_contracts
    df_contracts = pd.DataFrame(all_contracts, columns=['contract_time'])
    df_contracts = df_contracts[df_contracts['contract_time'] != '']
    contract_counts = df_contracts['contract_time'].value_counts().reset_index()
    contract_counts.columns = ['contract_time', 'count']
    return contract_counts

# ===============================
# LAMBDA HANDLER
# ===============================

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    input_bucket = 'job-market-source-bucket-rushi'
    output_bucket = 'job-market-destination-bucket-rushi'
    prefix = 'job_data/'
    timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')

    response = s3.list_objects_v2(Bucket=input_bucket, Prefix=prefix)
    json_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.json')]

    if not json_files:
        return {'statusCode': 500, 'body': 'No JSON files found in source bucket.'}

    input_key = sorted(json_files, reverse=True)[0]

    response = s3.get_object(Bucket=input_bucket, Key=input_key)
    raw_data = json.loads(response['Body'].read())

    adzuna_jobs = raw_data.get("adzuna", [])
    if isinstance(adzuna_jobs, dict):
        adzuna_jobs = adzuna_jobs.get("results", [])

    arbeitnow_jobs = raw_data.get("arbeitnow", [])
    if isinstance(arbeitnow_jobs, dict):
        arbeitnow_jobs = arbeitnow_jobs.get("results", [])

    df_adzuna = pd.json_normalize(adzuna_jobs)
    df_arbeitnow = pd.json_normalize(arbeitnow_jobs)
    df_all_jobs = pd.concat([df_adzuna, df_arbeitnow], ignore_index=True, sort=False)

    df_all_jobs = unify_company_names(df_all_jobs)
    title_counts_df = extract_job_title_counts(adzuna_jobs, arbeitnow_jobs)
    job_postings_per_day_df = count_job_postings_per_day(adzuna_jobs, arbeitnow_jobs)
    job_postings_per_month_df = count_job_postings_per_month(adzuna_jobs, arbeitnow_jobs)
    top_companies_df = top_hiring_companies(adzuna_jobs, arbeitnow_jobs)
    location_counts_df = location_analysis(adzuna_jobs, arbeitnow_jobs)
    contract_counts_df = contract_type_distribution(adzuna_jobs, arbeitnow_jobs)

    # Save files with folders
    cleaned_key = f'processed/cleaned_job_data/cleaned_job_data_{timestamp}.csv'
    cleaned_buffer = StringIO()
    df_all_jobs.to_csv(cleaned_buffer, index=False)
    s3.put_object(Bucket=output_bucket, Key=cleaned_key, Body=cleaned_buffer.getvalue())

    title_key = f'processed/job_title_counts/job_title_counts_{timestamp}.csv'
    title_buffer = StringIO()
    title_counts_df.to_csv(title_buffer, index=False)
    s3.put_object(Bucket=output_bucket, Key=title_key, Body=title_buffer.getvalue())

    postings_day_key = f'processed/job_postings_per_day/job_postings_per_day_{timestamp}.csv'
    postings_day_buffer = StringIO()
    job_postings_per_day_df.to_csv(postings_day_buffer, index=False)
    s3.put_object(Bucket=output_bucket, Key=postings_day_key, Body=postings_day_buffer.getvalue())

    postings_month_key = f'processed/job_postings_per_month/job_postings_per_month_{timestamp}.csv'
    postings_month_buffer = StringIO()
    job_postings_per_month_df.to_csv(postings_month_buffer, index=False)
    s3.put_object(Bucket=output_bucket, Key=postings_month_key, Body=postings_month_buffer.getvalue())

    top_companies_key = f'processed/top_hiring_companies/top_hiring_companies_{timestamp}.csv'
    top_companies_buffer = StringIO()
    top_companies_df.to_csv(top_companies_buffer, index=False)
    s3.put_object(Bucket=output_bucket, Key=top_companies_key, Body=top_companies_buffer.getvalue())

    location_key = f'processed/location_analysis/location_analysis_{timestamp}.csv'
    location_buffer = StringIO()
    location_counts_df.to_csv(location_buffer, index=False)
    s3.put_object(Bucket=output_bucket, Key=location_key, Body=location_buffer.getvalue())

    contract_key = f'processed/contract_type_distribution/contract_type_distribution_{timestamp}.csv'
    contract_buffer = StringIO()
    contract_counts_df.to_csv(contract_buffer, index=False)
    s3.put_object(Bucket=output_bucket, Key=contract_key, Body=contract_buffer.getvalue())

    return {
        'statusCode': 200,
        'message': 'Transformations completed and files saved to S3.',
        'source_file': input_key,
        'cleaned_job_data_key': cleaned_key,
        'job_title_counts_key': title_key,
        'job_postings_per_day_key': postings_day_key,
        'job_postings_per_month_key': postings_month_key,
        'top_hiring_companies_key': top_companies_key,
        'location_analysis_key': location_key,
        'contract_type_distribution_key': contract_key,
        'total_records': len(df_all_jobs)
    }
