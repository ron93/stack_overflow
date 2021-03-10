import json
import os
from datetime import datetime, timedelta

import requests
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

from jinja2 import Environment, FileSystemLoader



def call_api() -> dict:
    '''get first 50 questions'''
    question_url = Variable.get('STACK_OVERFLOW_QUESTION_URL')

    today = datetime.now()
    three_daya_ago = today - timedelta(days=7)
    two_days_ago = today - timedelta(days=5)

    payload = {
        "fromdate" : int(datetime.timestamp(three_daya_ago)),
        "todate": int(datetime.timestamp(two_days_ago)),
        "sort" : "votes",
        "site" : "stackoverflow",
        "order" : "desc",
        "tagged": Variable.get("TAG"),
        "client_id" : Variable.get("STACK_OVERFLOW_CLIENT_ID"),
        "client_secret" : Variable.get("STACK_OVERFLOW_KEY") 
    }

    response = request.get(question_url, params=payload ) 
    
    for question in response.json().get("items",[]):
        yield {
             "question_id": question["question_id"],
            "title": question["title"],
            "is_answered": question["is_answered"],
            "link": question["link"],
            "owner_reputation": question["owner"].get("reputation", 0),
            "score": question["score"],
            "tags": question["tags"],
        }

def insert_question_to_db():
    """insert questions to postgres database"""
    query = """
        INSERT INTO public.questions (
            question_id,
            title,
            is_answered,
            link,
            owner_reputation, 
            +
            score, 
            tags)
        VALUES (%s, %s, %s, %s, %s, %s, %s); 
        """
    rows = call_api()
    for row in rows:
        row = tuple(row.valuess())
        pg_hook = PostgresHook(postgres_conn_id="postgres_connection")
        pg_hook.run(insert_question_to_db, parameters=row)

def filter_question() -> str:
    """query questions from db and filter and return json
    [
        {
        "title": "Question Title",
        "is_answered": false,
        "link": "https://stackoverflow.com/questions/0000001/...",
        "tags": ["tag_a","tag_b"],
        "question_id": 0000001
        },
    ]
    """
    columns = ("title" , "is_answered" , "link","tags","question_id")
    filter_query = """
        SELECT title,  "is_answered" , "link","tags","question_id"
        FROM questions
        WHERE  score >=1 and owner_reputation > 1000
    """

    pg_hook = PostgresHook(postgres_conn_id="postgres_connection").get_conn()

    with pg_hook.cursor("serverCursor") as pg_hook:
        pg_cursor.execute(filter_query)
        rows = pg_cursor.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
        return json.dumps(results, index=2)