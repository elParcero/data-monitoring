import sqlite3
import dataset

conn = sqlite3.connect('beamlines.db')
db = dataset.connect("sqlite:///database.db")
beamline_name = 'chx'
table = db[beamline_name]
table.insert(dict(time="2017-01-01", file_size=100, plan_name="count", detector_name="eiger4m_single"))
