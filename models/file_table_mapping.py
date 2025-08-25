from models.raw import *

file_table_mapping = [
    ("MedPassResults", MedPassResults, "*MedPassResults*.csv"),
    ("VitalResults", VitalResults, "*VitalResults*.csv"),
    ("TasksApptsActivity", TasksApptsActivity, "*TasksApptsActivity*.csv"),
    ("PhysicalAssessmentWithClass", PhysicalAssessmentWithClass, "*PhysicalAssessment*.csv"),
    ("RosterReleases", RosterReleases, "*RosterReleases*.csv"),
    ("TBTestResults", TBTestResults, "*TBTestResults*.csv"),
    ("FormResponseCaptures", FormResponseCaptures, "*FormResponseCaptures*.csv"),
    ("FormSubmissions", FormSubmissions, "*FormSubmissions*.csv"),
    ("LabResults", LabResults, "*LabResults*.csv"),
    ("ActiveVitalTxOrders", ActiveVitalTxOrders, "*ActiveVitalandTXOrders*.csv"),
    ("ActiveRoster", ActiveRoster, "*ActiveRoster*.csv"),
    ("ActiveProbs", ActiveProbs, "*ActiveProbs*.csv"),
    ("ActiveOrdersWithRxNorm", ActiveOrdersWithRxNorm, "*ActiveORDERSWithRXNorm*.csv")
]