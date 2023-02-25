from __future__ import generators    # needs to be at the top of your module

import pandas as pd
import logging
import sys
import os
import csv
import re
import pickle

from sqlalchemy import create_engine

import psycopg2
import pytz

from tqdm import tqdm
from datetime import datetime, timedelta, timezone

#########################################################
# Log Module                                            #
#########################################################
import logging
import sys
import time
logging.basicConfig(level=logging.INFO, 
                    datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    handlers=[logging.FileHandler(sys.argv[0] + '.log'),
                              logging.StreamHandler()]
                    )
#########################################################
logging.info("Script de Migracion de TableSpaces PSQL - v0.1")
#########################################################
Question = input("Desea utilizar ultima conexion? (presione y) ")
if Question == ("y"):
  with open('lastdbconn.pkl','rb') as f: 
    host,bd,bduser,bdpass = pickle.load(f)
else:
  ## defino base de datos
  host = input("Ingrese el host de la base de datos ")
  ## defino base de datos
  bd = input("Ingrese la base de datos ")
  ## defino el usuario
  bduser = input("Ingrese el usuario de la db ")
  ## defino la pass
  bdpass = input("Ingrese la password de la db ")
  logging.info(f'Datos ingresados: BD {bd} / Host {host} / User {bduser} / Pass {bdpass}')
  #me guardo las variables en un archivo para recuperar ultima ejec
  with open('lastdbconn.pkl', 'wb') as f:  # Python 3: open(..., 'wb')
      pickle.dump([host,bd,bduser,bdpass], f)

tablaTarget = input("Escriba el nombre de la tabla a modificar ")
tablaSchema = input("Escriba el nombre del schema de la tabla ")

#########################################################
# Variables                                             #
#########################################################

PATH = os.getcwd() + '\\'
PATH_DOWNLOAD = 'G:\ enter local side path' + '\\'
PATH_SQLSIDE = '\\\\ enter remote side path \\'
#define un conector sql
conn = psycopg2.connect(f"host={host} dbname={bd} user={bduser} password={bdpass}")

#########################################################

def ResultIter(cursor, arraysize=1):
  'An iterator that uses fetchmany to keep memory usage down'
  while True:
    results = cursor.fetchmany(arraysize)
    if not results:
      break
    for result in results:
      yield result

#########################################################
logging.info(f'Verifico y elijo el tablespace')
######################################################################################################################
curs = conn.cursor()
curs.execute(f"""
 select coalesce(tablespace,'NA') as tablespace 
 from pg_tables 
 where schemaname = '{tablaSchema}' 
 and tablename like '{tablaTarget}_%' 
 group by 1;
""" 
)
logging.info(f'Listo los tablespaces de cada particion')

tablaCheck = curs.fetchall()
for row in tablaCheck:
  logging.info(f'|--> {row[0]}')
######################################################################################################################
curs = conn.cursor()
curs.execute(f"""
  SELECT spcname FROM pg_tablespace;
""" 
)
logging.info(f'Listo los tablespaces disponibles')

tablaCheck = curs.fetchall()
for row in tablaCheck:
  logging.info(f'|--> {row[0]}')

tablaNewTspace = input("Escriba el nombre del tablespace nuevo de la tabla ")

logging.info(f'La tabla ingresada es : {tablaTarget}')
pregunta2 = input("Desea continuar? (presione y para continuar) ")
if pregunta2 == ("y"):
  logging.info(f'Start')
  ######################################################################################################################
  curs = conn.cursor()
  curs.execute(f"""
    SELECT 
    table_name
    FROM (
            SELECT c.oid,nspname AS table_schema, relname AS table_name
            FROM pg_class c
            LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE relkind = 'r' 
            and relname like '{tablaTarget}_%' 
            and nspname = '{tablaSchema}'
            order by table_name asc
    ) a
  """ 
  )
  logging.info(f'|--> Checking last table')

  tablaCheck = curs.fetchall()
  for row in tablaCheck:
    logging.info(f'|--> {row[0]}')

  Question = input("Desea continuar? (presione y) ")
  if Question == ("y"):
    ###################
    for row in tablaCheck:
      logging.info(f'|--> Target Table: {row[0]} - Fecha: {datetime.now()}')
      #quit()
      ######################################################################################################################
      # LOAD TO DATABASE
      ######################################################################################################################
      logging.info(f'|--> Moving')
      connp = psycopg2.connect(f"host={host} dbname={bd} user={bduser} password={bdpass}")
      curp = connp.cursor()
      sql = f"""
        ALTER TABLE {tablaSchema}.{row[0]} SET TABLESPACE {tablaNewTspace};
      """

      curp.execute(sql)
      connp.commit()
      logging.info(f'|--> Moving table {row[0]}')
logging.info(f'|--> Done All')