
from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug,
            "columns": None
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):
        print (self._data)
        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0,CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                cols = self._data.get("columns", None)
                if cols is None:
                    cols = r.keys()
                    self._data["columns"] = list(cols)
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        Not required for the Assignment.
        :return: None
        """
        """dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)
        field_name = self._data["columns"]

        writer = csv.DictWriter(full_name, fieldnames=field_name)
        for r in self._rows:
            writer.writeheader()
            writer.writerow(r)"""

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

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
        tmp = self.key_to_template(key_fields)
        return self.find_by_template(tmp)


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
        result = []
        for r in self._rows:
            if self.matches_template(r, template):
                result.append(r)
        return result

    def delete_by_key(self, key_fields):

        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        create a new list. iterate it. if a row does not match, insert it into a new row. If it matches, skip it.
        """

        key_cols = self._data.get('key_columns', None)
        if key_cols is None:
            raise ValueError("Find by key but you did not define a key.")
        return self.delete_by_template(key_fields)

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        result = []
        num_row = sum(1 for row in self._rows)
        for r in self._rows:
            if not self.matches_template(r, template):
                result.append(r)
        self._rows = result
        return num_row-sum(1 for row in result)

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        key_cols = self._data.get('key_columns', None)
        if key_cols is None:
            raise ValueError("Find by key but you did not define a key.")

        tmp = self.key_to_template(key_fields)
        return self.update_by_template(tmp, new_values)

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """

        """find the row, delete it, and insert it. If it fails, put the original one back."""
        result = []
        for r in self._rows:
            if self.matches_template(r, template):
                tmp = self.find_by_template(template)
                self.delete_by_template(template)
                num_inserted_row = self.insert(new_values)
                if num_inserted_row is not 0:   #The insert function returns 1 if it successfully inserts. Otherwise, it returns 0.
                    print("Update completed.")
                    return num_inserted_row

                self.insert(tmp)
                print("Update failed.")
                return 0
        print("No match found for the template")
        return 0


    def insert(self, new_record):
        """
        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None (or number of rows inserted)
        """
        result = 0
        num_initial_row = sum(1 for row in self._rows)
        if new_record is None:
            raise ValueError("New value not inserted.")

        new_cols = set(new_record.keys())
        tbl_cols = set(self._data["columns"])

        if not new_cols.issubset(tbl_cols):
            raise ValueError("Unknown feature for the inserted value.")

        key_cols = self._data.get("key_columns", None)
        """Field 'playerID' doesn't have a default value."""
        if key_cols is not None:
            key_cols = set(key_cols)
            if not key_cols.issubset(new_cols):
                raise ValueError("Unknown key feature inserted.")
            for k in key_cols:
                if new_record.get(k, None) is None:
                    raise ValueError("Null value can not be given to a key feature.")

            key_tmp = self.key_to_template(new_record)
            if self.find_by_template(key_tmp) is not None\
                and len(self.find_by_template(key_cols)) > 0:
                raise ValueError("Duplicated value(s) found.")

        self._rows.append(new_record)
        return sum(1 for row in self._rows) - num_initial_row

    def get_rows(self):
        return self._rows

    def key_to_template(self, k_values):
        tmp = dict(zip(self._data.get('key_columns', None), k_values))
        return tmp
