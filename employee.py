import json

class Employee:
    def __init__(self, emp_dept='', emp_division='', emp_position='', emp_hire_date='', emp_salary=0):
        self.emp_dept = emp_dept
        self.emp_division = emp_division
        self.emp_position = emp_position
        self.emp_hire_date = emp_hire_date
        self.emp_salary = emp_salary

    @staticmethod
    def from_csv_line(line):
        return Employee(
            emp_dept=line[0],
            emp_division=line[1],
            emp_position=line[3],
            emp_hire_date=line[5],
            emp_salary=int(float(line[7]))
        )

    @staticmethod
    def from_json(json_str):
        data = json.loads(json_str)
        return Employee(
            emp_dept=data.get('emp_dept', ''),
            emp_division=data.get('emp_division', ''),
            emp_position=data.get('emp_position', ''),
            emp_hire_date=data.get('emp_hire_date', ''),
            emp_salary=data.get('emp_salary', 0)
        )

    def to_json(self):
        return json.dumps(self.__dict__)
