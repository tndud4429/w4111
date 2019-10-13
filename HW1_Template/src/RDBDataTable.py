from src.BaseDataTable import BaseDataTable
import json
import pymysql
import pymysql.cursors
import pandas as pd

pd.set_option("display.width", 196)
pd.set_option('display.max_columns', 16)


class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        if table_name is None or connect_info is None:
            raise ValueError("Invalid input.")

        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns
        }

        cnx = self.get_connection(connect_info)
        if cnx is not None:
            self._cnx = cnx
        else:
            raise Exception("Could not get a connection.")

    def __str__(self):

        result = "RDBDataTable:\n"
        result += json.dumps(self._data, indent=2, default=str)

        row_count = self.get_row_count()
        result += "\nNumber of rows = " + str(row_count)

        some_rows = pd.read_sql(
            "select * from " + self._data["table_name"] + " limit 10",
            con=self._cnx
        )
        result += "First 10 rows = \n"
        result += str(some_rows)

        return result

    def get_row_count(self):

        row_count = self._data.get("row_count", None)
        if row_count is None:
            sql = "select count(*) as count from " + self._data["table_name"]
            res, d = self.run_q(sql, args=None, fetch=True, conn=self._cnx, commit=True)
            row_count = d[0]['count']
            self._data['row_count'] = row_count

        return row_count

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        key_cols = self._data.get('key_columns', None)
        if key_cols is None:
            raise ValueError("Find by key but you did not define a key.")
        tmp = dict(zip(key_cols,key_fields))
        result = self.find_by_template(template=tmp, field_list=field_list)

        if result is not None and len(result)>0:
            result = result[0]
        else:
            result = None
        return result


    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        w_clause, args = self.template_to_where_clause(template)
        sql = "select " + self.get_select_fields(field_list) + " from " + self.get_table_name() + " " + w_clause
        #cur = self._cnx.cursor()
        #cur.execute(sql, args)
        #result = cur.fetchall()    ###These commands do the same thing as self.run_q function.
        result, data  = self.run_q(sql,args=args, conn=self._cnx, commit=True, fetch=True)
        return data

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        if self._data["key_columns"] is None:
            raise ValueError("Find by key but you did not define a key.")
        result = self.delete_by_template(template=key_fields)
        return result

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        w_clause, args = self.template_to_where_clause(template)
        sql = "delete from " + self.get_table_name() + " " + w_clause
        result, data = self.run_q(sql, args=args, conn=self._cnx, commit=True, fetch=True)
        return result

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        key_cols = self._data.get('key_columns', None)
        if key_cols is None:
            raise ValueError("Find by key but you did not define a key.")
        tmp = dict(zip(key_cols, key_fields))
        result = self.update_by_template(template=tmp, new_values=new_values)
        return result

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """

        sql, args = self.create_update(table_name=self._data["table_name"], template=template, changed_cols=new_values)
        res, data = self.run_q(sql, args=args, conn=self._cnx, fetch=False)
        return res

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        sql, args = self.create_insert(self._data['table_name'], new_record)
        result, data = self.run_q(sql, args=args, fetch=False, conn=self._cnx, commit=True)
        return result


    def get_rows(self):
        return self._rows

    def template_to_where_clause(self, template):

        if template is None or template == {}:
            w_clause = None
            args = None
        else:
            terms = []
            args = []
            for k, v in template.items():
                terms.append(k + "=%s")
                args.append(v)

            w_clause = "where " + (" and ".join(terms))

        return w_clause, args

    def template_to_set_clause(self, template):

        if template is None or template == {}:
            w_clause = None
            args = None
        else:
            terms = []
            args = []
            for k, v in template.items():
                terms.append(k + "=%s")
                args.append(v)

            s_clause = "set " + (" and ".join(terms))

        return s_clause, args

    def get_select_fields(self, fields):

        if fields is None or fields == []:
            field_list = " * "
        else:
            field_list = ",".join(fields)

        return field_list

    def get_table_name(self):
        return self._data["table_name"]

    def get_connection(self, connect_info):
        """

        :param connect_info: A dictionary containing the information necessary to make a PyMySQL connection.
        :return: The connection. May raise an Exception/Error.
        """

        cnx = pymysql.connect(**connect_info)
        return cnx

    def run_q(self, sql, args=None, fetch=True, cur=None, conn=None, commit=True):
        '''
        Helper function to run an SQL statement.

        This is a modification that better supports HW1. An RDBDataTable MUST have a connection specified by
        the connection information. This means that this implementation of run_q MUST NOT try to obtain
        a defailt connection.

        :param sql: SQL template with placeholders for parameters. Canno be NULL.
        :param args: Values to pass with statement. May be null.
        :param fetch: Execute a fetch and return data if TRUE.
        :param conn: The database connection to use. This cannot be NULL, unless a cursor is passed.
            DO NOT PASS CURSORS for HW1.
        :param cur: The cursor to use. This is wizard stuff. Do not worry about it for now.
            DO NOT PASS CURSORS for HW1.
        :param commit: This is wizard stuff. Do not worry about it.

        :return: A pair of the form (execute response, fetched data). There will only be fetched data if
            the fetch parameter is True. 'execute response' is the return from the connection.execute, which
            is typically the number of rows effected.
        '''

        cursor_created = False
        connection_created = False

        try:

            #if conn is None:
            #    raise ValueError("In this implementation, conn cannot be None.")

            if cur is None:
                cursor_created = True
                cur = conn.cursor()

            if args is not None:
                log_message = cur.mogrify(sql, args)
            else:
                log_message = sql

            #logger.debug("Executing SQL = " + log_message)

            res = cur.execute(sql, args)

            if fetch:
                data = cur.fetchall()
            else:
                data = None

            # Do not ask.
            if commit == True:
                conn.commit()

        except Exception as e:
            raise (e)

        return (res, data)

    def create_insert(self, table_name, new_row):

        sql = "insert into " + table_name + " "
        cols = list(new_row.keys())
        cols = ",".join(cols)
        col_clause = "(" + cols + ")"

        args = list(new_row.values())

        s_stuff = ["%s"]*len(args)
        s_clause = ",".join(s_stuff)
        v_clause = " values("+s_clause + ")"

        sql += " " + col_clause + " " + v_clause
        return sql, args

    def create_update(self, table_name, template, changed_cols):

        sql = "update " + table_name + " "
        set_terms = []
        args = []
        for k, v in changed_cols.items():
            args.append(v)
            set_terms.append(k + "=%s")
        set_terms = ",".join(set_terms)
        set_clause = "set " + set_terms

        w_clause, args2 = self.template_to_where_clause(template)

        sql += set_clause + " " + w_clause
        args.extend(args2)
        return sql, args

