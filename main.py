# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

main_db = pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
# Replace None with your code
df_boston = pd.read_sql("""
                        SELECT firstName, lastName
                        FROM employees 
                        JOIN offices
                            USING(officeCode)
                        WHERE city = 'Boston'""", conn)
print(df_boston)

# STEP 2
# Replace None with your code
df_zero_emp = pd.read_sql("""
                            SELECT  officeCode
                            FROM offices
                            LEFT JOIN employees
                                USING(officeCode)
                            WHERE employeeNumber IS NULL""", conn)
print(df_zero_emp)

# STEP 3
# Replace None with your code
df_employee = pd.read_sql("""
                            SELECT  firstName,
                                    lastName, 
                                    city,
                                    state 
                            FROM employees 
                            JOIN offices
                                USING(officeCode)
                            ORDER BY firstName, lastName""", conn)
print(df_employee)

# STEP 4
# Replace None with your code
df_contacts = pd.read_sql("""
                            SELECT  customerNumber,
                                    customerName,
                                    contactFirstName,
                                    contactLastName 
                            FROM customers
                            LEFT JOIN orders
                                USING(customerNumber)
                            WHERE orderNumber IS NULL
                            ORDER BY contactLastName""", conn)
print(df_contacts)

# STEP 5
# Replace None with your code
df_payment = pd.read_sql("""
                            SELECT  contactFirstName,
                                    contactLastName,
                                    amount,
                                    paymentDate 
                            FROM customers
                            JOIN payments
                                USING(customerNumber)
                            ORDER BY cast(amount AS INTEGER) DESC""", conn)
print(df_payment)

# STEP 6
# Replace None with your code
df_credit = pd.read_sql("""
                            SELECT  employeeNumber,
                                    firstName,
                                    lastName,
                                    COUNT(customerNumber) AS num_customers
                            FROM employees
                            JOIN customers
                                ON employees.employeeNumber = customers.salesRepEmployeeNumber
                            GROUP BY employeeNumber
                            HAVING AVG(creditLimit) > 90000
                            ORDER BY num_customers DESC""", conn)
print(df_credit)

# STEP 7
# Replace None with your code
df_product_sold = pd.read_sql("""
                            SELECT  productName,
                                    COUNT(orderNumber) AS numorders,
                                    SUM(quantityOrdered) AS totalunits
                            FROM products
                            JOIN orderdetails
                                USING(productCode)
                            GROUP BY productName
                            ORDER BY totalunits DESC""", conn)
print(df_product_sold)

# STEP 8
# Replace None with your code
df_total_customers = pd.read_sql("""
                            SELECT  productName,
                                    productCode,
                                    COUNT(DISTINCT customerNumber) AS numpurchasers
                            FROM products
                            JOIN orderdetails
                                USING(productCode)
                            JOIN orders
                                USING(orderNumber)
                            JOIN customers
                                USING(customerNumber)
                            GROUP BY productName
                            ORDER BY numpurchasers DESC""", conn)
print(df_total_customers)

# STEP 9
# Replace None with your code
df_customers = pd.read_sql("""
                            SELECT  officeCode,
                                    offices.city,
                                    COUNT(customerNumber) AS n_customers
                            FROM offices
                            JOIN employees
                                USING(officeCode)
                            JOIN customers
                                ON employees.employeeNumber = customers.salesRepEmployeeNumber
                            GROUP BY officeCode
                            ORDER BY officeCode""", conn)
print(df_customers)

# STEP 10
# Replace None with your code
df_under_20 = pd.read_sql("""
                            WITH ProductCustomerCounts AS (
                                SELECT  productCode,
                                        COUNT(DISTINCT customerNumber) AS customer_count
                                FROM orderdetails
                                JOIN orders
                                    USING(orderNumber)
                                GROUP BY productCode
                            )
                            SELECT DISTINCT employeeNumber,
                                    firstName,
                                    lastName,
                                    offices.city,
                                    officeCode
                            FROM employees
                            JOIN offices
                                USING(officeCode)
                            JOIN customers
                                ON employees.employeeNumber = customers.salesRepEmployeeNumber
                            JOIN orders
                                USING(customerNumber)
                            JOIN orderdetails
                                USING(orderNumber)
                            JOIN ProductCustomerCounts
                                ON orderdetails.productCode = ProductCustomerCounts.productCode
                            WHERE customer_count < 20
                            ORDER BY lastName, firstName""", conn)
print(df_under_20)

conn.close()