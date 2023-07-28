from sqlalchemy import create_engine, text
import os

db_connection_params = os.environ['DB_CONNECTION_STRING']
engine = create_engine(db_connection_params,
                       connect_args={"ssl": {
                         "ssl_ca": "/etc/ssl/cert.pem",
                       }})


def load_jobs_from_db():
  with engine.connect() as conn:
    result = conn.execute(text("select * from jobs"))
    jobs = []
    for row in result.all():
      jobs.append(row._asdict())
    return jobs


def load_job_from_db(id):
  with engine.connect() as conn:
    result = conn.execute(text("select * from jobs where jobs.id = :val"),
                          dict(val=id))
    rows = result.all()
    if len(rows) == 0:
      return None
    else:
      return rows[0]._asdict()
