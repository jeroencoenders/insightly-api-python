"""Insightly API module"""

import datetime
import dateutil.relativedelta
from functools import partial

import click

from .api_helpers import retrieve_api_item_by_id
from .database_helpers import retrieve_data, retrieve_data_by_id
from .models import Database
from .settings import LABELFORMAT, MAX_RESCAN_COUNT, API_URL

class Insightly:
    """
    Insightly v3 API
    # Usage:
    # Verbosity
    # 0 No output
    # 1 Just status messages
    # 2 Settings
    # 3 Detailled status messages
    # 4 Debug level
    """
    database_initialised = False
    database_connected = False

    def check_database_connection(self):
        """
        Checks the database. If not ok, it raises an exception.
        """
        if not self.database or not self.database_initialised:
            raise Exception("Database not initialised")

        if not self.database_connected:
            raise Exception("Database not connected")

        if not self.database.manager:
            raise Exception("Database manager not connected")

    def unmark(self):
        """
        Unmarks every object in the database
        """
        self.check_database_connection()

        if self.database.manager.verbose >= 1:
            click.secho('Removing all dirty and rescan marks on every '\
                        'object in the database', fg='green')
            for object_to_load in self.database.manager.models:
                object_to_load.update({object_to_load.rescan: False,
                                       object_to_load.dirty: False}).execute()
                click.secho('Removed rescan marks on {}'
                            .format(object_to_load.class_description))

    def connect_and_initialise(self, \
                               apikey=None,
                               verbose=0,
                               max_rescan_count=MAX_RESCAN_COUNT):
        """
        Connects the cache database
        """
        self.database = Database()
        self.database.connect()
        self.database_connected = True

        self.database.manager.init(apikey=apikey, \
                                   verbose=verbose,
                                   max_rescan_count=max_rescan_count)
        self.database_initialised = True

    def set_to_debug(self):
        """Sets database to debug level"""
        self.database.manager.verbose = 4
        self.database.manager.debug = True

    def reset_database(self):
        """Resets the database to an empty database"""
        self.check_database_connection()
        if self.database.manager.verbose >= 1:
            click.secho('Resetting full database', fg='green')
        self.database.drop()
        self.database.create()

    def rescan(self, rescan_object):
        """
        Marking every object in the database to be rescanned.
        """
        self.check_database_connection()
        if rescan_object == '':
            if self.database.manager.verbose >= 1:
                click.secho('Marking every object in the database to be '\
                            'rescanned', fg='green')
                object_to_loads = self.database.manager.models
        else:
            model = None
            for model in self.database.manager.models:
                if model.class_description == rescan_object:
                    break
            else:
                click.secho('Could not find {} in the database'
                            .format(rescan_object), fg='red')
                return
            click.secho('Marking every {} object in the database to be rescanned'
                        .format(rescan_object), fg='green')
            object_to_loads = model

        # Marking objects for rescan
        for object_to_load in object_to_loads:
            if object_to_load.mark_for_rescan:
                object_to_load.update({object_to_load.rescan: True,
                                       object_to_load.dirty: True}).execute()
                click.secho('Marked {} for rescan'.format(object_to_load.class_description))
        # if self.database.manager.verbose >= 1:
        #     click.secho('Now processing the entire database', fg='green')

    def rescan_lastmonth(self, rescan_object):
        """
        Marking objects of last month in the database to be rescanned.
        """
        self.check_database_connection()
        pastdate = datetime.datetime.now() + dateutil.relativedelta.relativedelta(months=-1)
        if rescan_object == '':
            if self.database.manager.verbose >= 1:
                click.secho('Marking objects of last month in the database to be '\
                            'rescanned ({:%Y-%m-%d})'.format(pastdate), fg='green')
                object_to_loads = self.database.manager.models
        else:
            model = None
            for model in self.database.manager.models:
                if model.class_description == rescan_object:
                    break
            else:
                click.secho('Could not find {} in the database'
                            .format(rescan_object), fg='red')
                return
            click.secho('Marking {} objects of the last month in the database to be rescanned'
                        .format(rescan_object), fg='green')
            object_to_loads = model

        # Marking objects for rescan
        for object_to_load in object_to_loads:
            if object_to_load.mark_for_rescan:
                object_to_load.update({object_to_load.rescan: True,
                                       object_to_load.dirty: True}) \
                              .where(object_to_load.last_updated > pastdate) \
                              .execute()
                click.secho('Marked {} for rescan'.format(object_to_load.class_description))
        # if self.database.manager.verbose >= 1:
        #     click.secho('Now processing the entire database', fg='green')

    def sync(self, \
             quickscan=True):
        """
        Synchronises the cache database with the API
        """
        self.check_database_connection()
        timestamp = datetime.datetime.now()
        for object_to_load in self.database.manager.models:
            if 'load' in dir(object_to_load):
                object_to_load.load(object_to_load, timestamp, quickscan=quickscan)

        # Rescanning the leftovers
        if not self.database.manager.rescans_expired:
            if self.database.manager.verbose >= 3:
                click.secho('Rescanning the left-over rescan objects in the '\
                            'database', fg='green')
            with click.progressbar(self.database.manager.models,
                                   label=LABELFORMAT.format('Rescanning left-over rescan objects')
                                   ) as pbar:
                for object_to_load in pbar:
                    for rescan_obj in object_to_load.select() \
                                                    .where(object_to_load.rescan == True).execute():
                        if (self.database.manager.rescan_count <= self.database.manager.max_rescan_count):
                            if self.database.manager.verbose >= 1:
                                click.secho('Rescanning \"{}\" ({}) (counter={:02}/{:02})' \
                                            .format(rescan_obj.__description__,
                                                    object_to_load.__name__,
                                                    self.database.manager.rescan_count,
                                                    self.database.manager.max_rescan_count), \
                                                    fg='cyan')
                            try:
                                item = retrieve_api_item_by_id(API_URL, \
                                                               object_to_load.class_api_url,
                                                               rescan_obj.uid,
                                                               apikey=self.database.manager.apikey)
                                if item is None:
                                    # TODO: Something should still happen here so that a real rescan takes place.
                                    rescan_obj.dirty = True
                                else:
                                    rescan_obj.dirty = False
                            except Exception as err:
                                click.secho("{}".format(err))
                                rescan_obj.dirty = False
                            rescan_obj.rescan = False
                            rescan_obj.save()
                            self.database.manager.rescan_count += 1
                        else:
                            self.database.manager.rescans_expired = True
                            if self.database.manager.verbose >= 3:
                                click.secho('Rescan of \"{}\" ({}) encountered, but rescan ' \
                                            'counter has expired (counter={:02}/{:02})'
                                            .format(rescan_obj.__description__,
                                                    object_to_load.__name__,
                                                    self.database.manager.rescan_count,
                                                    self.database.manager.max_rescan_count),
                                            fg='red')

            # How many rescan objects are still left?
            if self.database.manager.verbose >= 4:
                click.secho('Listing the number of rescan objects in the database', fg='green')
                for object_to_load in self.database.manager.models:
                    totalcount = object_to_load.select().count()
                    rescancount = object_to_load.select() \
                                                .where(object_to_load.rescan == True).count()
                    click.secho('{}: {} of {} with rescan status ({:.1f}%).'
                                .format(object_to_load.class_description, \
                                        str(rescancount), str(totalcount), \
                                        rescancount*100/totalcount), fg='red')

            # Deleting all dirty objects
            if not self.database.manager.rescans_expired:
                if self.database.manager.verbose >= 3:
                    click.secho('Deleting all dirty objects in the database', fg='green')
                dryrun = False
                if dryrun:
                    for object_to_load in self.database.manager.models:
                        results = object_to_load.select() \
                                                .where((object_to_load.dirty == True) &
                                                       (object_to_load.rescan == False)) \
                                                .execute()
                        for obj_to_delete in results:
                            click.secho("Object to delete: {} ({})" \
                                        .format(obj_to_delete.__description__,
                                                obj_to_delete.uid), fg='red')
                else:
                    for object_to_load in self.database.manager.models:
                        object_to_load.delete() \
                                       .where((object_to_load.dirty == True) &
                                              (object_to_load.rescan == False)) \
                                       .execute()
                        # click.secho("Object deleted: {} ({})" \
                        #             .format(obj_to_delete.__description__,
                        #                     obj_to_delete.uid), fg='red')

    def connect_dynamic_endpoints(self):
        """
        Connects the dynamic endpoints ot the class
        """
        self.check_database_connection()
        for object_to_load in self.database.manager.models:
            # Setting property on API object
            setattr(self, object_to_load.class_property, retrieve_data(object_to_load))
            setattr(self, '{}__byid'.format(object_to_load.class_property), \
                          partial(retrieve_data, self, object_to_load))

            # Setting property on API object for subclasses
            for subclass in object_to_load.subclasses:
                setattr(self, subclass.class_property, retrieve_data(subclass))
                setattr(self, '{}__byid'.format(subclass.class_property), \
                              partial(retrieve_data_by_id, subclass))

    def __init__(self, \
                 apikey=None, \
                 verbose=0, \
                 max_rescan_count=MAX_RESCAN_COUNT):

        # Connect the database
        self.connect_and_initialise(apikey, verbose, max_rescan_count)
        self.connect_dynamic_endpoints()
