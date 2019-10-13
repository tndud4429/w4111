# W4111_F19_HW1
Implementation template for homework 1.

CSVDataTable file:
    find_by_template(): iterate through the rows and if any row matches using matches_template function, return them all as a list.
    find_by_key(): using zip() and dict() functions, make the key in a dictionary form so that it can be fed to the find_by_template() function.
    delete_by_template(): iterate through the rows and if any row does not match using matches_template function, return them all as a list.
    delete_by_key(): like find_by_key function, make the key into a dictionary form and feed it into the delete_by_template function.
    update_by_template(): iterate through the rows and if any row matches, save the info in the temporary variable, delete the row, insert the new information. If insert fails, then insert the original value back into the file.
    update_by_key(): make the key into a dictionary form, and feed it into update_by_template function.
    insert(): first, check if the new value is empty. If not, check if the new table columns are a subset of existing columns. If it is, check if the key values are null. If not, check if duplicated value is found. If not, append the new_record into the existing table. Otherwise, raise Value Errors.

RDBDataTable file:
    Basically, it constructs a string that is equivalent to the queries for mysql, and feed the string into ren_q function which execute and fetch using cursor.
    
The implementations work as intended.