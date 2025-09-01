from models.raw import *

header_to_columns_active_orders_with_rxnorm = {
    "FACILITY_ID": "facility_id",
    "UNIT": "unit",
    "NPI Number of ordering Practitioner": "npi_number_of_ordering_practitioner",
    "Name of Ordering Provider": "name_of_ordering_provider",
    "Date & Time order Received": "date_time_order_received",
    "NDC Code of Drug ordered": "ndc_code_of_drug_ordered",
    "Drug Name": "drug_name",
    "Effective Date": "effective_date",
    "Dosage": "dosage",
    "Drug Strength": "drug_strength",
    "Frequency": "frequency",
    "Duration": "duration",
    "Route": "route",
    "Number of Refills Issued": "number_of_refills_issued",
    "Expiration Date": "expiration_date",
    "Provider Admin Instructions": "provider_admin_instructions",
    "Dispensed Quantity": "dispensed_quantity",
    "Formulary Or Non Formulary Drug Flag": "formulary_or_non_formulary_drug_flag",
    "Keep On Person Indicator": "keep_on_person_indicator",
    "What time of day the medication should be taken at (AM, PM, NOON, BEDTIME)": "medication_time_of_day",
    "ORDER_ID": "order_id",
    "ORDERED_BY": "ordered_by"
}

header_to_columns_active_probs = {
    "SAPPHIRE_PAT_ID": "sapphire_pat_id",
    "FACILITY_ID": "facility_id",
    "CODE_SET": "code_set",
    "CODE": "code",
    "LONG_DESCRIPTION": "long_description",
    "DATE_CREATED": "date_created"
}

header_to_columns_active_roster = {
    "SAPPHIRE_PAT_ID": "sapphire_pat_id",
    "FACILITY": "facility",
    "FACILITY_ID": "facility_id",
    "JMS_ID": "jms_id",
    "PATIENT_NAME": "patient_name",
    "UNIT": "unit",
    "WING": "wing",
    "ROOM": "room",
    "BED": "bed",
    "DOB": "dob",
    "GENDER": "gender",
    "LAST_BOOKED_DATE": "last_booked_date",
    "LAST_RELEASE_DATE": "last_release_date",
    "ATA_STATUS": "ata_status"
}

header_to_columns_active_vital_tx_orders = {
    "SAPPHIRE_PAT_ID": "sapphire_pat_id",
    "FACILITY_ID": "facility_id",
    "VITAL_ORDER_TYPE": "vital_order_type",
    "Directions": "directions",
    "FREQUENCY": "frequency",
    "TIME_SLOTS": "time_slots",
    "START_DATE": "start_date",
    "STOP_DATE": "stop_date",
    "PRESCRIBER": "prescriber",
    "NPI": "npi",
    "VITAL_ORDER_ID": "vital_order_id",
    "ORDERED_BY": "ordered_by"
}

header_to_columns_form_response_captures = {
    "SAPPHIRE_PAT_ID": "sapphire_pat_id",
    "FACILITY_ID": "facility_id",
    "FormName": "form_name",
    "CAP_DESC": "cap_desc",
    "ANS_TEXT": "ans_text",
    "DATE_CREATED": "date_created"
}

header_to_columns_form_submissions = {
    "SAPPHIRE_PAT_ID": "sapphire_pat_id",
    "FACILITY_ID": "facility_id",
    "DATE_FORM_SUBMITTED": "date_form_submitted",
    "FORM_NAME": "form_name"
}

header_to_columns_lab_results = {
    "SAPPHIRE_PAT_ID": "sapphire_pat_id",
    "FACILITY_ID": "facility_id",
    "SERVICE_TEXT": "service_text",
    "OBS_TEXT": "obs_text",
    "OBS_RESULT": "obs_result",
    "SPEC_RCD_ON": "spec_rcd_on"
}

header_to_columns_med_pass_results = {
    "SAPPHIRE_PAT_ID": "sapphire_pat_id",
    "FACILITY_ID": "facility_id",
    "ORDER_ID": "order_id",
    "PRN_FLAG": "prn_flag",
    "KOP_FLAG": "kop_flag",
    "LABEL_NAME": "label_name",
    "ALT_NAME": "alt_name",
    "STRENGTH": "strength",
    "DOS_ID": "dos_id",
    "FORMATTED_NDC": "formatted_ndc",
    "MEDPASS_DATE": "medpass_date",
    "RESULT_DATE": "result_date",
    "MEDPASS_RESULT": "medpass_result",
    "QTY_DOSE": "qty_dose",
    "RECORDED_BY_FULL_NAME": "recorded_by_full_name"
}

header_to_columns_physical_assessment = {
    "SAPPHIRE_PAT_ID": "sapphire_pat_id",
    "FACILITY_ID": "facility_id",
    "LAST_BOOKED_DATE": "last_booked_date",
    "LAST_PA": "last_pa",
    "LAST_PCHP": "last_pchp",
    "LAST_PAHP": "last_pahp"
}

header_to_columns_roster_releases = {
    "SAPPHIRE_PAT_ID": "sapphire_pat_id",
    "FACILITY": "facility",
    "FACILITY_ID": "facility_id",
    "JMS_ID": "jms_id",
    "PATIENT_NAME": "patient_name",
    "UNIT": "unit",
    "WING": "wing",
    "ROOM": "room",
    "BED": "bed",
    "DOB": "dob",
    "GENDER": "gender",
    "LAST_BOOKED_DATE": "last_booked_date",
    "LAST_RELEASE_DATE": "last_release_date"
}

header_to_columns_tasks_appts_activity = {
    "SAPPHIRE_PAT_ID": "sapphire_pat_id",
    "FACILITY_ID": "facility_id",
    "ITEM_TYPE": "item_type",
    "STATUS": "status",
    "ITEM_CREATED_ON_DATE": "item_created_on_date",
    "ITEM_DUE_DATE": "item_due_date",
    "ITEM_COMPLETED_ON": "item_completed_on",
    "ITEM_NOTE": "item_note",
    "ITEM_TAGS": "item_tags"
}

header_to_columns_tb_test_results = {
    "SAPPHIRE_PAT_ID": "sapphire_pat_id",
    "FACILITY_ID": "facility_id",
    "TESTED_BY": "tested_by",
    "PPD_TST_DT": "ppd_tst_dt",
    "PPD_TST_READ_DT": "ppd_tst_read_dt",
    "REACTION_SIZE": "reaction_size",
    "READ_BY": "read_by",
    "READER_COMMENTS": "reader_comments",
    "NEXT_TST_DT": "next_tst_dt",
    "MOST_RECENT_TB_XRAY_RESULT": "most_recent_tb_xray_result"
}

header_to_columns_vital_results = {
    "SAPPHIRE_PAT_ID": "sapphire_pat_id",
    "FACILITY_ID": "facility_id",
    "VITAL_TYPE": "vital_type",
    "VITAL_VALUE1": "vital_value1",
    "VITAL_VALUE2": "vital_value2",
    "VITAL_UM": "vital_um",
    "RECORDED_BY_FULL_NAME": "recorded_by_full_name",
    "RESULT_DATE": "result_date"
}

file_table_mapping = [
    ("MedPassResults", MedPassResults, "*MedPassResults*.csv", header_to_columns_med_pass_results),
    ("VitalResults", VitalResults, "*VitalResults*.csv", header_to_columns_vital_results),
    ("TasksApptsActivity", TasksApptsActivity, "*TasksApptsActivity*.csv", header_to_columns_tasks_appts_activity),
    ("PhysicalAssessmentWithClass", PhysicalAssessmentWithClass, "*PhysicalAssessment*.csv", header_to_columns_physical_assessment),
    ("RosterReleases", RosterReleases, "*RosterReleases*.csv", header_to_columns_roster_releases),
    ("TBTestResults", TBTestResults, "*TBTestResults*.csv", header_to_columns_tb_test_results),
    ("FormResponseCaptures", FormResponseCaptures, "*FormResponseCaptures*.csv", header_to_columns_form_response_captures),
    ("FormSubmissions", FormSubmissions, "*FormSubmissions*.csv", header_to_columns_form_submissions),
    ("LabResults", LabResults, "*LabResults*.csv", header_to_columns_lab_results),
    ("ActiveVitalTxOrders", ActiveVitalTxOrders, "*ActiveVitalandTXOrders*.csv", header_to_columns_active_vital_tx_orders),
    ("ActiveRoster", ActiveRoster, "*ActiveRoster*.csv", header_to_columns_active_roster),
    ("ActiveProbs", ActiveProbs, "*ActiveProbs*.csv", header_to_columns_active_probs),
    ("ActiveOrdersWithRxNorm", ActiveOrdersWithRxNorm, "*ActiveORDERSWithRXNorm*.csv", header_to_columns_active_orders_with_rxnorm)
]