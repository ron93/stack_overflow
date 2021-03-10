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