import sqlite3


DB_TIMEOUT=600
DB_INIT="""
    CREATE TABLE IF NOT EXISTS sources (
        path TEXT PRIMARY KEY
    );
    CREATE TABLE IF NOT EXISTS tags (
        question INT NOT NULL,
        tag TEXT NOT NULL,
        PRIMARY KEY(question, tag)
    );
    CREATE TABLE IF NOT EXISTS views (
        question INT NOT NULL,
        viewcount INT NOT NULL,
        date TEXT NOT NULL,
        PRIMARY KEY(date, question)
    );
    CREATE TABLE IF NOT EXISTS meta (

        key TEXT PRIMARY KEY,
        value TEXT
    );
"""
DB_SELECT_SOURCE="SELECT 1 FROM sources WHERE path = :path"
DB_INSERT_SOURCE="INSERT OR REPLACE INTO sources(path, status) VALUES(:path, :status)"
DB_INSERT_TAG="INSERT OR IGNORE INTO tags(question, tag) VALUES(:question, :tag)"
DB_INSERT_VIEWCOUNT="INSERT OR IGNORE INTO views(question, date, viewcount) VALUES(:question, :date, :viewcount)"



class Db:
    def __init__(self, uri):
        db = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=DB_TIMEOUT)

        self.cursor = cursor = db.cursor()
        cursor.executescript(DB_INIT)

        self.loadMetadata()
        self.setMetadata('version', 1)

    #
    # Metadata
    #
    def loadMetadata(self):
        CURR_DB_VERSION = 1

        def updateToVersion1():
            cursor.executescript("""
                ALTER TABLE sources ADD COLUMN status INT DEFAULT 1;
                UPDATE meta SET value=1 where KEY='version';
            """)

        cursor = self.cursor
        updater = (
            updateToVersion1,
        )

        while True:
            self.db_version = int(self.getMetadata('version', 0))
            if self.db_version == CURR_DB_VERSION:
                break

            updater[self.db_version]()

    def getMetadata(self, key, default=None):
        cursor = self.cursor

        cursor.execute("SELECT value FROM meta WHERE key=:key", locals())
        result = cursor.fetchall()
        if result:
            ((value,),) = result
            return value

        if default is not None:
            self.setMetadata(key, default)

        return default

    def setMetadata(self, key, value):
        cursor = self.cursor

        cursor.execute("INSERT OR REPLACE INTO meta(key, value) VALUES (:key, :value)", locals())

    #
    # API
    #
    def exists(self, path):
        cursor = self.cursor

        cursor.execute(DB_SELECT_SOURCE, dict(path=path))
        result = cursor.fetchall()

        if not result:
            return None

        return result[0][0]

    def write(self, entries):
        cursor = self.cursor

        try:
            cursor.execute("BEGIN DEFERRED TRANSACTION")
            for entry in entries:
                cursor.execute(DB_INSERT_SOURCE, dict(path=entry['path'], status=entry['status']))
                for item in entry['items']:
                    cursor.execute(DB_INSERT_VIEWCOUNT, dict(
                        question=item['id'],
                        date=item['date'],
                        viewcount=item['viewcount']
                    ))
                    for tag in item['tags']:
                        cursor.execute(DB_INSERT_TAG, dict(
                            question=item['id'],
                            tag=tag
                        ))
            cursor.execute("COMMIT")
            del entries[:]
            print("COMMIT")

        except Exception as e:
            print("ROLLBACK")
            cursor.execute("ROLLBACK")
            raise
