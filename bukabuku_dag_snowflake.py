from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain
from datetime import datetime
import pandas as pd
import os
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
import requests
import time
import numpy as np


header = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
}

categories_list = {'44': 'anak-anak', '176': 'arsitektur-design-interior', '46': 'bahasa-kamus', '48': 'biografi', '47': 'bisnis-manajemen-keuangan', '424': 'buku-impor', '200': 'buku-saku', '129': 'current-affairs-reportage', '272': 'design', '185': 'ensiklopedia', '98': 'fashion-beauty', '49': 'fiksi', '50': 'filsafat', '51': 'fotografi', '52': 'hobi-interest', '172': 'hospitality', '206': 'humanitas', '251': 'inspirasional-spiritualitas', '333': 'just-for-fun', '205': 'katalog', '54': 'kesehatan', '256': 'kisah-nyata', '92': 'komik', '253': 'komputer', '58': 'kuliner', '203': 'life-style', '255': 'majalah', '244': 'menulis-jurnalisme', '142': 'musik-perfilman', '376': 'non-fiksi', '209': 'non-profit-and-philantrophy', '59': 'orang-tua-keluarga', '296': 'pengembangan-diri-motivasi', '60': 'pertanian-perkebunan', '282': 'peternakan-perikanan', '61': 'politik-hukum', '130': 'psikologi', '295': 'puisi-sastra', '252': 'referensi', '143': 'relationship-weddings', '149': 'religius', '190': 'sains-teknologi', '147': 'sejarah', '144': 'seni-budaya', '148': 'textbooks', '155': 'travel', '194': 'umum'}

def get_book_detail(book_url, ctg):
    keys = ['ISBN13', 'judul', 'author', 'kode_kategori', 'kategori', 'ketersediaan', 'harga_asli', 
               'discount', 'harga_diskon', 'Format', 'Tanggal Terbit', 'Bahasa', 'Penerbit', 
               'Halaman', 'Dimensi', 'link_buku', 'deskripsi']
    book_detail = {key : None for key in keys}
    
    try:
        book_req = requests.get(book_url)
        soup = BeautifulSoup(book_req.text)
        detail = soup.find('div', class_ = 'product_detail')
        book_detail['link_buku'] = book_url

        try:
            book_detail['judul'] = soup.find('span', class_ = 'product_title').getText().strip()
        except:
            book_detail['judul'] = None
        try:
            book_detail['author'] = soup.find('span', class_ = 'product_author').find('a').getText().strip()
        except:
            book_detail['author'] = None
        book_detail['kode_kategori'] = list(categories_list.keys())[list(categories_list.values()).index(ctg)]
        book_detail['kategori'] = ctg
        try:
            book_detail['ketersediaan'] = detail.find('span', class_ = 'product_status_orange').getText(strip=True)
            book_detail['harga_asli'] = detail.find('span', class_ = 'price_discounted').getText(strip=True)
            book_detail['discount'] = detail.find('span', class_ = 'discount_percent').getText(strip=True)
            book_detail['harga_diskon'] = detail.find('span', class_ = 'price').getText(strip=True)
        except:
            book_detail['ketersediaan'] = detail.find('span', class_ = 'product_status_red').getText(strip=True)
            #book_detail['harga'] = None
            #book_detail['discount'] = None

        detail_buku = detail.find_all('table')[-1]
        for i in detail_buku.find_all('tr'):
            book_detail[i.find_all('td')[0].getText(strip=True)] = i.find_all('td')[2].getText(strip=True)
        
        try:
            book_detail['deskripsi'] = soup.find('div', class_ = 'product_description').getText(strip=True)
        except:
            book_detail['deskripsi'] = None
        
        #if book_detail['ISBN13'] != None:
        #    book_detail['ISBN13'] = str(book_detail['ISBN13'])
        
        book_detail = {key: book_detail[key] for key in keys}
    except:
        pass
    
    return book_detail

filepath = os.getcwd()
#  print(filepath)

def get_all_books_data(ti, code_cat):

    ctg_ids = code_cat

    df = pd.DataFrame()

    for ctg_id in ctg_ids:
        ctg_name = categories_list[ctg_id]
        main_url = f'http://www.bukabuku.com/browses/index/cid:{ctg_id}/c:{ctg_name}'
        req = requests.get(main_url)
        print('REQUEST STATUS:', req.status_code)
        soup = BeautifulSoup(req.text)
        print(main_url)
        number_of_pages = len(soup.find('select', {'id' : 'page_selector'}).find_all('option'))
        print("ID, Category, pages:", ctg_id, ctg_name, number_of_pages)

        print("Scraping started ..... ")
        print("Pages progress:", end = ' ')
        start = time.time()
        for page in range(1,number_of_pages+1):
            main_url_page = main_url+f'/page:{page}/'
            req = requests.get(main_url_page)
            #print('page requested:', page)

            soup = BeautifulSoup(req.text)
            books = soup.find('div', class_= 'content_container').find('div', class_ = 'main_section').find_all('div', class_ = 'content')
            book_urls = []
            for book in books:
                next_links = book.find('a')['href']
                book_url = f'http://www.bukabuku.com{next_links}'
                book_urls.append(book_url)

            current_page_data = pd.DataFrame(get_book_detail(url, ctg_name) for url in book_urls)
            concated = [df, current_page_data]
            df = pd.concat(concated, ignore_index=True)
            print('=>', page, end = ' ')
        end = time.time()
        print(f"\nCategory {ctg_name} data retrieved successfully! ", format(end - start, '.2f'), "s\n")

    #df.to_csv(f"{filepath}/pipeline_files/bukabuku_{ctg_name}.csv", index = False)
    #print("CSV created!")
    #ti.xcom_push(key = 'category', value = ctg_name)
    ti.xcom_push(key = 'books_data_extracted', value = df.to_json())

#code number for each books category
code_cats = [['176', '48'], ['92', '253'], ['61','149'], ['190','147'], ['47', '376']]

def merge_data(ti):
    dfx_json = ti.xcom_pull(key = 'books_data_extracted', task_ids = [f'get_all_books_data_{i+1}' for i in range(len(code_cats))])

    dfx = pd.concat([pd.read_json(i) for i in dfx_json], ignore_index=True)

    data_merged = dfx.to_json()
    ti.xcom_push(key = 'df_merged', value = data_merged)
    

def transform_books_data(ti):
    dff_json = ti.xcom_pull(key = 'df_merged', task_ids = 'merge_task')

    df = pd.read_json(dff_json)

    df.dropna(subset=['ISBN13'], inplace=True)
    df = df.drop(columns=['discount', 'harga_diskon', 'Dimensi'])

    df['ISBN13'] = df['ISBN13'].astype(str).str.replace('.0','')
    df['harga_asli'] = df['harga_asli'].str.replace('Rp. ', '').str.replace('.', '').astype('Int64')
    df['harga_asli'] = df['harga_asli'].replace(0, np.nan)

    df['Tanggal Terbit'] = df['Tanggal Terbit'].str[-4:]
    df.rename(columns={'Tanggal Terbit': 'tahun_Terbit'}, inplace=True)
    df['Halaman'] = df['Halaman'].astype('Int64')
    df = df.sort_values(by=['judul'])
    df = df.reset_index()
    df = df.drop(columns=['index'])
    df.columns = df.columns.str.lower()

    print(df.iloc[0])
    ti.xcom_push(key = 'data_transformed', value = df.to_json())
    df.to_csv(f"{filepath}/pipeline_files/books_data_transformed_short.csv", index = False)
    print("Data transformed!")

def load_books_data_postgres(ti):
    df_json = ti.xcom_pull(key='data_transformed', task_ids='transform_books_data')
    df = pd.read_json(df_json)
    
    # Get the PostgreSQL connection using PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='local_postgres')
    conn = postgres_hook.get_conn()
    
    try:
        cur = conn.cursor()

        cur.execute("""
            DROP TABLE IF EXISTS books_data_postgres;
            CREATE TABLE books_data_postgres (
                isbn13 VARCHAR(40),
                judul VARCHAR(300),
                author VARCHAR(170),
                kode_kategori VARCHAR(5),
                kategori VARCHAR(55),
                ketersediaan VARCHAR(30),
                harga_asli INT,
                format VARCHAR(45),
                tahun_terbit VARCHAR(10),
                bahasa VARCHAR(48),
                penerbit VARCHAR(110),
                halaman INT,
                link_buku VARCHAR(500),
                deskripsi TEXT
            );
        """)

        cur.execute("DELETE FROM books_data_postgres;")
        conn.commit()

        # Using SQLAlchemy engine from PostgresHook
        engine = postgres_hook.get_sqlalchemy_engine()
        df.to_sql('books_data_postgres', con=engine, if_exists='append', index=False)
        print("Data ready in postgresql!")
    
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    
    finally:
        cur.close()
        conn.close()

#DAGs setup

default_args = {
    'owner' : 'Shiqo',
    'start_date': datetime(2024,1,1),
    'retries': 1
}

dag = DAG(
    'bukabuku_etl_snowflake',
    default_args = default_args,
    description = "A DAG to extract data from bukabuku.com",
    schedule_interval='00 17 * * *',
    max_active_runs = 1,
    catchup = False
)


begin_task = DummyOperator(task_id = 'begin', dag = dag)


list_of_create_task = []
for i in range(len(code_cats)):
    extract_task_bukabuku = PythonOperator( 
        task_id = f'get_all_books_data_{i+1}',
        python_callable = get_all_books_data,
        op_kwargs = {"code_cat" : code_cats[i]},
        dag=dag)
    list_of_create_task.append(extract_task_bukabuku)

merge_task = PythonOperator(
    task_id = 'merge_task',
    python_callable = merge_data,
    dag=dag)

transform_task_bukabuku = PythonOperator( 
    task_id = 'transform_books_data',
    python_callable = transform_books_data,
    dag=dag
)

filepath = os.getcwd()
source_file_path = f"{filepath}/pipeline_files/books_data_transformed_short.csv"
SF_STAGE = 'bukabuku_staging'
SQL_PUT_FILE = f"""REMOVE @bukabuku_staging; 
                PUT file:///{source_file_path} @{SF_STAGE};"""


create_table_snowflake = """
            DROP TABLE IF EXISTS books_data_snowflake;
            CREATE TABLE books_data_snowflake (
                isbn13 VARCHAR(40),
                judul VARCHAR(300),
                author VARCHAR(170),
                kode_kategori VARCHAR(5),
                kategori VARCHAR(55),
                ketersediaan VARCHAR(30),
                harga_asli INT,
                format VARCHAR(45),
                tahun_terbit VARCHAR(10),
                bahasa VARCHAR(48),
                penerbit VARCHAR(110),
                halaman INT,
                link_buku VARCHAR(500),
                deskripsi TEXT
            );
        """

create_t_snowflake_task = SnowflakeOperator(
    task_id = 'create_table_snowflake',
    snowflake_conn_id = 'snowflake_conn',
    sql = create_table_snowflake,
    dag = dag
)

load_stage_task = SnowflakeOperator(
    task_id="load_snowflake_stage",
    snowflake_conn_id="snowflake_conn",
    sql=SQL_PUT_FILE,
    database="db_shiqo",
    schema="PUBLIC",
    dag=dag
)

load_postgres_task = PythonOperator( 
    task_id = 'load_books_postgres',
    python_callable = load_books_data_postgres,
    dag=dag
)

copy_into_t_snowflake = CopyFromExternalStageToSnowflakeOperator(
    task_id="copy_into_t_snowflake",
    snowflake_conn_id="snowflake_conn",
    database="db_shiqo",
    schema="PUBLIC",
    table='books_data_snowflake',
    stage='bukabuku_staging',
    file_format="(type = 'CSV',field_delimiter = ',', skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')",
)

end_task = DummyOperator(task_id = 'end', dag=dag)

chain(begin_task, list_of_create_task, merge_task, transform_task_bukabuku, 
      [load_postgres_task, create_t_snowflake_task, load_stage_task], copy_into_t_snowflake, end_task)