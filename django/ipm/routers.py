class MeuRoteador:
    def db_for_read(self, model, **hints):
        if model._meta.app_label == 'seuapp':
            if 'oracle_secondary' in model._meta.db_table:
                return 'oracle_secondary'
            elif 'mysql_database' in model._meta.db_table:
                return 'mysql_database'
        return None

    def db_for_write(self, model, **hints):
        if model._meta.app_label == 'seuapp':
            if 'oracle_secondary' in model._meta.db_table:
                return 'oracle_secondary'
            elif 'mysql_database' in model._meta.db_table:
                return 'mysql_database'
        return None

    def allow_relation(self, obj1, obj2, **hints):
        return (
            (obj1._meta.app_label == 'seuapp' and 'oracle' in obj1._meta.db_table and
             obj2._meta.app_label == 'seuapp' and 'oracle' in obj2._meta.db_table) or
            (obj1._meta.app_label == 'seuapp' and 'mysql' in obj1._meta.db_table and
             obj2._meta.app_label == 'seuapp' and 'mysql' in obj2._meta.db_table)
        )

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if app_label == 'seuapp':
            if db == 'oracle_secondary' or db == 'mysql_database':
                return True
            elif db == 'default':
                return False
        return None