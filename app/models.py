# app/models.py

from datetime import datetime
from .extensions import db


class EmployeeRecord(db.Model):
    __tablename__ = 'employee_records'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    assessment_id = db.Column(db.String(100))
    assessment_name = db.Column(db.String(500))
    name = db.Column(db.String(200))
    email = db.Column(db.String(200))
    employee_id = db.Column(db.String(100), index=True)
    final_completion_date = db.Column(db.String(100))
    issuer = db.Column(db.String(200))
    level = db.Column(db.String(50))
    marks = db.Column(db.String(50))
    qualifier = db.Column(db.String(200))
    skill = db.Column(db.String(500))
    skill_id = db.Column(db.String(100))
    valid_till = db.Column(db.String(100))
    wipro_function = db.Column(db.String(200))

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
