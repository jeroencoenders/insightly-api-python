"""Database helpers module"""

import json

def retrieve_data(dbclass):
    """Retrieves data from the database and returns it as a JSON object"""
    query = dbclass.select()
    return [json.loads(obj.json) for obj in query]

def retrieve_data_by_id(dbclass, uid):
    """Retrieves a single data item from the database and returns it as a JSON object"""
    query = dbclass.select().where(dbclass.uid == uid)
    return [json.loads(obj.json) for obj in query]

def update_model_in_database(cls, apidata, last_updated, last_retrieved):
    """Updates a model"""
    cls.json = json.dumps(apidata)
    cls.last_updated = last_updated
    cls.last_retrieved = last_retrieved
    cls.dirty = False
    cls.rescan = False
    cls.save()
