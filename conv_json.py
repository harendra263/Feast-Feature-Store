import pandas as pd


emp = pd.read_csv("EmployeeData.csv")

dept = pd.read_csv("Department.csv")


print(emp)
print(dept)

emp.to_json("employee.json")

dept.to_json("department.json")