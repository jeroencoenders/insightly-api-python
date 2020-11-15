"""Manager module"""

from .settings import PAGESIZE, MAX_RESCAN_COUNT, API_URL

class DatabaseManager:
    """Database manager"""
    api_url = API_URL
    apikey = ''
    models = []
    pagesize = PAGESIZE
    verbose = 0
    debug = False
    max_rescan_count = MAX_RESCAN_COUNT
    rescan_count = 1
    rescans_expired = False

    def init(self,
             apikey=None,
             verbose=0,
             pagesize=PAGESIZE, \
             max_rescan_count=MAX_RESCAN_COUNT):
        """Initialise the database manager"""
        self.apikey = apikey
        self.verbose = verbose
        self.pagesize = pagesize
        self.max_rescan_count = max_rescan_count

    def register(self, cls):
        """Register class with the database manager"""
        if not cls in self.models:
            self.models.append(cls)

    def get_model_by_class_property(self, model):
        """Retrieves a model"""
        for model_to_check in self.models:
            if model_to_check.class_property == model:
                return model_to_check
        return None
