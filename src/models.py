"""Models"""

import datetime
from datetime import timedelta
import json

from peewee import Model, IntegerField, TextField, DateTimeField, BooleanField
from playhouse.sqlite_ext import SqliteExtDatabase
import click

from .api_helpers import retrieve_from_api, retrieve_page, build_url
from .database_helpers import update_model_in_database
from .manager import DatabaseManager
from .settings import DATABASE_FILE, LABELFORMAT

db = SqliteExtDatabase(DATABASE_FILE)
mngr = DatabaseManager()

def registered_database_model(cls):
    """Decorator which registers a model with the manager"""
    mngr.register(cls)
    cls.manager = mngr
    return cls

class BaseModel(Model):
    """Base model"""
    uid = IntegerField()
    json = TextField()
    last_updated = DateTimeField()
    last_retrieved = DateTimeField()
    rescan = BooleanField(default=False)
    dirty = BooleanField(default=False)

    mark_for_rescan = True
    class_description = None
    class_property = None
    class_api_url = None

    class_uid_key = None
    class_name_key = None
    class_update_key = None
    subclasses = []

    class Meta:
        """Meta class"""
        database = db

    def __getitem__(self, key):
        return json.loads(self.json)[key]

    @property
    def __description__(self):
        return json.loads(self.json)[self.class_name_key]

    def load_subclasses_primitive(self, subclass, obj, retrieval_timestamp):
        """Loads the subclasses of a primitive object"""
        if mngr.verbose >= 3:
            click.secho('Loading subclass {} for {} {}'
                        .format(subclass.class_description,
                                self.__name__, obj[self.class_uid_key]), fg='red')
        url = build_url(mngr.api_url, self.class_api_url, str(obj[self.class_uid_key]), subclass.class_api_url)
        _, data = retrieve_from_api(url, apikey=mngr.apikey)

        for apidata in data:
            cls = subclass.get_or_none(subclass.uid == apidata[subclass.class_uid_key])
            if cls is None:
                cls = subclass(uid=apidata[subclass.class_uid_key], \
                               json=json.dumps(apidata), \
                               last_updated=retrieval_timestamp,
                               last_retrieved=retrieval_timestamp,
                               rescan=False,
                               dirty=False)
                cls.save()
                if mngr.verbose >= 3:
                    click.secho('New {}: {}'
                                .format(subclass.__name__, cls[subclass.class_name_key]),
                                fg='magenta')
            else:
                update_model_in_database(cls, apidata, retrieval_timestamp, retrieval_timestamp)
                if mngr.verbose >= 3:
                    click.secho('{}: \"{}\" updated.'.format(subclass.__name__,
                                                             apidata[subclass.class_name_key]), \
                                                             fg='magenta')


    def load_subclasses_main(self, subclass, obj, retrieval_timestamp):
        if mngr.verbose >= 3:
            click.secho('Loading subclass {} for {} {}'.format(subclass.class_description, self.__name__, obj[self.class_uid_key]), fg='red')
        url = build_url(mngr.api_url, self.class_api_url, str(obj[self.class_uid_key]),subclass.class_api_url)
        _, data = retrieve_from_api(url, apikey=mngr.apikey)

        for apidata in data:
            cls = subclass.get_or_none(subclass.uid == apidata[subclass.class_uid_key])
            new_date = datetime.datetime.strptime(apidata[subclass.class_update_key], '%Y-%m-%d %H:%M:%S')
            if cls is None:
                cls=subclass(uid=apidata[subclass.class_uid_key], \
                             json=json.dumps(apidata), \
                             last_updated=new_date,
                             last_retrieved=retrieval_timestamp,
                             rescan=False,
                             dirty=False)
                cls.save()
                if mngr.verbose >= 1:
                    click.secho('New {}: {}'
                                .format(subclass.__name__,
                                        cls[subclass.class_name_key]), fg='magenta')
            else:
                old_date = cls.last_updated
                if new_date > old_date:
                    update_model_in_database(cls, apidata, new_date, retrieval_timestamp)
                    if mngr.verbose >= 1:
                        click.secho('{}: \"{}\" already exists but needed updating. ' \
                                     'New record update time: {:%Y-%m-%d %H:%M:%S}. ' \
                                     'Old record last retrieval: {:%Y-%m-%d %H:%M:%S}.' \
                                     .format(subclass.__name__,
                                             apidata[subclass.class_name_key],
                                             new_date, old_date), fg='magenta')
                else:
                    if cls.rescan:
                        update_model_in_database(cls, apidata, new_date, retrieval_timestamp)
                        if mngr.verbose >= 1:
                            click.secho('Rescanning: {}: \"{}\" already exists. Updating. New record update time: {:%Y-%m-%d %H:%M:%S}. Old record last retrieval: {:%Y-%m-%d %H:%M:%S}.'.format(subclass.__name__, apidata[subclass.class_name_key], new_date, old_date),fg='cyan')

                    else:
                        if mngr.verbose >= 3:
                            click.secho('{}: \"{}\" already exists. Ignoring. New record update time: {:%Y-%m-%d %H:%M:%S}. Old record last retrieval: {:%Y-%m-%d %H:%M:%S}.'.format(subclass.__name__, apidata[subclass.class_name_key], new_date, old_date),fg='cyan')

    def load_single(self, apidata, retrieval_timestamp):
        """Loads a single model"""
        cls = self.get_or_none(self.uid == apidata[self.class_uid_key])
        new_date = datetime.datetime.strptime(apidata[self.class_update_key], '%Y-%m-%d %H:%M:%S')
        if cls is None:
            cls=self(uid=apidata[self.class_uid_key], \
                     json=json.dumps(apidata), \
                     last_updated=new_date, \
                     last_retrieved=retrieval_timestamp,
                     rescan=False,
                     dirty=False)
            cls.save()
            if mngr.verbose >= 1:
                click.secho('New {}: {}'.format(self.__name__, cls[self.class_name_key]),fg='magenta')
            for subclass in self.subclasses:
                subclass.load_subclasses(self, subclass, cls, retrieval_timestamp)
        else:
            old_date = cls.last_updated
            if new_date > old_date:
                update_model_in_database(cls, apidata, new_date, retrieval_timestamp)
                if mngr.verbose >= 1:
                    click.secho('{}: \"{}\" already exists but needed updating. New record update time: {:%Y-%m-%d %H:%M:%S}. Old record last retrieval: {:%Y-%m-%d %H:%M:%S}.'.format(self.__name__, apidata[self.class_name_key], new_date, old_date),fg='magenta')
                for subclass in self.subclasses:
                    subclass.load_subclasses(self, subclass, cls, retrieval_timestamp)
            else:
                if cls.rescan:
                    if (mngr.rescan_count <= mngr.max_rescan_count):
                        if mngr.verbose >= 1:
                            click.secho('Rescanning \"{}\" ({}) (counter={:02}/{:02})'.format(apidata[self.class_name_key], self.__name__, mngr.rescan_count, mngr.max_rescan_count), fg='cyan')
                        for subclass in self.subclasses:
                            subclass.load_subclasses(self, subclass, cls, retrieval_timestamp)
                            mngr.rescan_count = mngr.rescan_count+1
                        update_model_in_database(cls, apidata, new_date, retrieval_timestamp)
                    else:
                        mngr.rescans_expired = True
                        if mngr.verbose >= 3:
                            click.secho('Rescan of \"{}\" ({}) encountered, but rescan counter has expired (counter={:02}/{:02})'.format(apidata[self.class_name_key], self.__name__, mngr.rescan_count, mngr.max_rescan_count), fg='red')
                else:
                    if mngr.verbose >= 3:
                        click.secho('{}: \"{}\" already exists. Ignoring. New record update time: {:%Y-%m-%d %H:%M:%S}. Old record last retrieval: {:%Y-%m-%d %H:%M:%S}.'.format(self.__name__, apidata[self.class_name_key], new_date, old_date),fg='cyan')

        return cls

    def load_main(self, retrieval_timestamp, quickscan=True):
        if mngr.verbose >= 4:
            click.secho('=== load_main for {} ==='.format(self.class_description), fg='cyan')

        # If we not are doing a quickscan, retrieve all records
        database_class = None
        if not quickscan:
            if mngr.verbose >= 2:
                click.secho('Retrieving all {}.'.format(self.class_description), fg='green')
            datestr = ''
        else:
            # Check if class already has a record on when it was last retrieved
            database_class = DatabaseClass.get_or_none(dbclass=self.__name__)
            if database_class is None:
                if mngr.verbose >= 2:
                    click.secho('Quickscan: No record exists. Retrieving all {}.'.format(self.class_description), fg='green')
                datestr = ''
            else:
                if mngr.verbose >= 2:
                    click.secho('Quickscan: {} class last retrieved on {:%Y-%m-%d %H:%M:%S}.'.format(self.__name__, database_class.last_retrieved), fg='green')

                # Format retrieval timestamp so that it can be used on the API call
                retrieve = database_class.last_retrieved - timedelta(days=1)
                datestr = '{:%Y-%m-%dT%H:%M:%SZ}'.format(retrieve)

        # Retrieve the total count of records from the API
        totalcount, data = retrieve_page(mngr.api_url, self.class_api_url, skip=0, top=1, apikey=mngr.apikey, datestr=datestr)
        if mngr.verbose >= 3:
            click.secho('Retrieved totalcount={:d}'.format(totalcount), fg='yellow')

        # Retrieve the pages from the API
        lastpage = int(totalcount / mngr.pagesize)+1
        if mngr.debug:
            lastpage = 1
        with click.progressbar(range(0, lastpage),
                               label=LABELFORMAT.format('Downloading {} from Insightly'.format(self.class_description))) as bar:
            for page in bar:
                totalcount, data=retrieve_page(mngr.api_url, self.class_api_url, skip=page*mngr.pagesize, top=mngr.pagesize, apikey=mngr.apikey, datestr=datestr)
                if mngr.verbose >= 3:
                    click.secho('Downloaded page {:d} (totalcount={:d}, skip={:d}, top={:d})'.format(page, totalcount, page*mngr.pagesize, mngr.pagesize), fg='yellow')

                for dataitem in data:
                    obj = self.load_single(self, dataitem, retrieval_timestamp)

        # Saving Master record to record when the records were last retrieved
        if database_class is None:
            database_class = DatabaseClass(dbclass=self.__name__, \
                                           last_retrieved=retrieval_timestamp)
        else:
            database_class.last_retrieved = retrieval_timestamp
        database_class.save()

    def load_single_primitive(self, apidata, retrieval_timestamp):
        cls = self.get_or_none(self.uid == apidata[self.class_uid_key])
        if cls is None:
            cls=self(uid=apidata[self.class_uid_key], \
                     json=json.dumps(apidata), \
                     last_updated=retrieval_timestamp, \
                     last_retrieved=retrieval_timestamp,
                     rescan=False,
                     dirty=False)
            cls.save()
            if mngr.verbose >= 3:
                click.secho('New {}: {}'.format(self.__name__, cls[self.class_name_key]),fg='magenta')
        else:
            update_model_in_database(cls, apidata, retrieval_timestamp, retrieval_timestamp)
            if mngr.verbose >= 3:
                click.secho('{} updated: {}'.format(self.__name__, cls[self.class_name_key]),fg='magenta')
        return cls

    def load_primitive(self, retrieval_timestamp, quickscan=None):
        if mngr.verbose >= 4:
            click.secho('=== load_primitive for {} ==='.format(self.class_description), fg='cyan')

        database_class = DatabaseClass.get_or_none(dbclass=self.__name__)
        if database_class is None:
            if mngr.verbose >= 3:
                click.secho('No record exists. Retrieving all {}.'.format(self.class_description), fg='green')
            datestr = ''
        else:
            if mngr.verbose >= 3:
                click.secho('{} class last retrieved on {:%Y-%m-%d %H:%M:%S}.'.format(self.__name__, database_class.last_retrieved), fg='green')

        # Retrieve the total count of records from the API
        totalcount, data = retrieve_page(mngr.api_url, self.class_api_url, skip=0, top=1, apikey=mngr.apikey)
        if mngr.verbose >= 3:
            click.secho('Retrieved totalcount={:d}'.format(totalcount), fg='yellow')

        # Retrieve the pages from the API
        lastpage = int(totalcount / mngr.pagesize)+1
        if mngr.debug:
            lastpage = 1
        with click.progressbar(range(0, lastpage),
                               label=LABELFORMAT.format('Downloading {} from Insightly'.format(self.class_description))) as bar:
            for page in bar:
                totalcount, data = retrieve_page(mngr.api_url, self.class_api_url, skip=page*mngr.pagesize, top=mngr.pagesize, apikey=mngr.apikey)
                if mngr.verbose >= 3:
                    click.secho('Downloaded page {:d} (totalcount={:d}, skip={:d}, top={:d})'.format(page, totalcount, page*mngr.pagesize, mngr.pagesize), fg='yellow')

                for dataitem in data:
                    obj = self.load_single_primitive(self, dataitem, retrieval_timestamp)

        # Saving Master record to record when the records were last retrieved
        if database_class is None:
            database_class = DatabaseClass(dbclass=self.__name__, \
                                           last_retrieved=retrieval_timestamp)
        else:
            database_class.last_retrieved = retrieval_timestamp
        database_class.save()

@registered_database_model
class Pipeline(BaseModel):
    """Pipeline model"""
    class_description = 'pipelines'
    class_property = 'pipelines'
    class_api_url = 'Pipelines'

    class_uid_key = 'PIPELINE_ID'
    class_name_key = 'PIPELINE_NAME'

    def load(self, *args, **kwargs):
        self.load_primitive(self, *args, **kwargs)

@registered_database_model
class PipelineStage(BaseModel):
    class_description = 'pipeline stages'
    class_property = 'pipeline_stages'
    class_api_url = 'PipelineStages'

    class_uid_key = 'STAGE_ID'
    class_name_key = 'STAGE_NAME'

    def load(self, *args, **kwargs):
        self.load_primitive(self, *args, **kwargs)

@registered_database_model
class User(BaseModel):
    class_description = 'users'
    class_property = 'users'
    class_api_url = 'Users'

    class_uid_key = 'USER_ID'
    class_name_key = 'FIRST_NAME'

    def load(self, *args, **kwargs):
        self.load_primitive(self, *args, **kwargs)

@registered_database_model
class OpportunityCategory(BaseModel):
    class_description = 'opportunity categories'
    class_property = 'opportunity_categories'
    class_api_url = 'OpportunityCategories'

    class_uid_key = 'CATEGORY_ID'
    class_name_key = 'CATEGORY_NAME'

    def load(self, *args, **kwargs):
        self.load_primitive(self, *args, **kwargs)

@registered_database_model
class Comment(BaseModel):
    class_description = 'comments'
    class_property = 'comments'
    class_api_url = 'Comments'

    class_uid_key = 'COMMENT_ID'
    class_name_key = 'COMMENT_ID'
    class_update_key = 'DATE_UPDATED_UTC'

    def load_subclasses(self, *args, **kwargs):
        self.load_subclasses_main(self, *args, **kwargs)

@registered_database_model
class Task(BaseModel):
    class_description = 'tasks'
    class_property = 'tasks'
    class_api_url = 'Tasks'

    class_uid_key = 'TASK_ID'
    class_name_key = 'TITLE'
    class_update_key = 'DATE_UPDATED_UTC'
    subclasses = [Comment]

    def load(self, *args, **kwargs):
        self.load_main(self, *args, **kwargs)

@registered_database_model
class OpportunityLinks(BaseModel):
    class_description = 'opportunity links'
    class_property = 'opportunity_links'
    class_api_url = 'Links'

    class_uid_key = 'LINK_ID'
    class_name_key = 'OBJECT_NAME'

    mark_for_rescan = False

    def load_subclasses(self, *args, **kwargs):
        self.load_subclasses_primitive(self, *args, **kwargs)

@registered_database_model
class Opportunity(BaseModel):
    class_description = 'opportunities'
    class_property = 'opportunities'
    class_api_url = 'Opportunities'

    class_uid_key = 'OPPORTUNITY_ID'
    class_name_key = 'OPPORTUNITY_NAME'
    class_update_key = 'DATE_UPDATED_UTC'
    subclasses=[OpportunityLinks]

    def load(self, *args, **kwargs):
        self.load_main(self, *args, **kwargs)

@registered_database_model
class Project(BaseModel):
    class_description = 'projects'
    class_property = 'projects'
    class_api_url = 'Projects'

    class_uid_key = 'PROJECT_ID'
    class_name_key = 'PROJECT_NAME'
    class_update_key = 'DATE_UPDATED_UTC'

    def load(self, *args, **kwargs):
        self.load_main(self, *args, **kwargs)

@registered_database_model
class Lead(BaseModel):
    class_description = 'leads'
    class_property = 'leads'
    class_api_url = 'Leads'

    class_uid_key = 'LEAD_ID'
    class_name_key = 'TITLE'
    class_update_key = 'DATE_UPDATED_UTC'

    def load(self, *args, **kwargs):
        self.load_main(self, *args, **kwargs)

@registered_database_model
class LeadStatus(BaseModel):
    class_description = 'lead statuses'
    class_property = 'lead_statuses'
    class_api_url = 'LeadStatuses'

    class_uid_key = 'LEAD_STATUS_ID'
    class_name_key = 'LEAD_STATUS'

    def load(self, *args, **kwargs):
        self.load_primitive(self, *args, **kwargs)

@registered_database_model
class EventLinks(BaseModel):
    class_description = 'event links'
    class_property = 'event_links'
    class_api_url = 'Links'

    class_uid_key = 'LINK_ID'
    class_name_key = 'OBJECT_NAME'

    mark_for_rescan = False

    def load_subclasses(self, *args, **kwargs):
        self.load_subclasses_primitive(self, *args, **kwargs)

@registered_database_model
class Event(BaseModel):
    class_description = 'events'
    class_property = 'events'
    class_api_url = 'Events'

    class_uid_key = 'EVENT_ID'
    class_name_key = 'TITLE'
    class_update_key = 'DATE_UPDATED_UTC'
    subclasses=[EventLinks]

    def load(self, *args, **kwargs):
        self.load_main(self, *args, **kwargs)

@registered_database_model
class EmailLinks(BaseModel):
    class_description = 'email links'
    class_property = 'email_links'
    class_api_url = 'Links'

    class_uid_key = 'LINK_ID'
    class_name_key = 'OBJECT_NAME'

    mark_for_rescan = False

    def load_subclasses(self, *args, **kwargs):
        self.load_subclasses_primitive(self, *args, **kwargs)

@registered_database_model
class Email(BaseModel):
    class_description = 'emails'
    class_property = 'emails'
    class_api_url = 'Emails'

    class_uid_key = 'EMAIL_ID'
    class_name_key = 'SUBJECT'
    class_update_key = 'EMAIL_DATE_UTC'
    subclasses=[Comment, EmailLinks]

    def load(self, *args, **kwargs):
        self.load_main(self, *args, **kwargs)

@registered_database_model
class Note(BaseModel):
    class_description = 'notes'
    class_property = 'notes'
    class_api_url = 'Notes'

    class_uid_key = 'NOTE_ID'
    class_name_key = 'TITLE'
    class_update_key = 'DATE_UPDATED_UTC'
    subclasses=[Comment]

    def load(self, *args, **kwargs):
        self.load_main(self, *args, **kwargs)

class DatabaseClass(Model):
    dbclass = TextField()
    last_retrieved = DateTimeField()

    class Meta:
        database = db

class Database:
    """Cache Database"""
    global db
    global mngr

    def connect(self):
        """Connects to the cache database"""
        db.connect()

    def create(self):
        """Create the tables in the cache database"""
        for table in mngr.models:
            if not table.table_exists():
                db.create_tables([table])
        if not DatabaseClass.table_exists():
            db.create_tables([DatabaseClass])

    def close(self):
        """Closes the connection to the cache database"""
        db.close()

    def drop(self):
        for table in mngr.models:
            if table.table_exists():
                db.drop_tables([table])
        if DatabaseClass.table_exists():
            db.drop_tables([DatabaseClass])

    def replace_table(self, table):
        if table.table_exists():
            db.drop_tables([table])
            db.create_tables([table])

    def create_table(self, table):
        if not table.table_exists():
            db.create_tables([table])

    @property
    def database(self):
        """Returns the database"""
        return db

    @property
    def manager(self):
        return mngr
