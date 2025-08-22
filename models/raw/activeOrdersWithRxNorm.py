from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class ActiveOrdersWithRxNorm(BaseModel):
    """Model for Active Orders with RxNorm data from incoming_*_ActiveORDERSWithRXNorm.csv"""
    facility_id: Optional[str] = Field(alias="FACILITY_ID", default=None)
    unit: Optional[str] = Field(alias="UNIT", default=None)
    npi_number_of_ordering_practitioner: Optional[str] = Field(alias="NPI Number of ordering Practitioner", default=None)
    name_of_ordering_provider: Optional[str] = Field(alias="Name of Ordering Provider", default=None)
    date_time_order_received: Optional[str] = Field(alias="Date & Time order Received", default=None)
    ndc_code_of_drug_ordered: Optional[str] = Field(alias="NDC Code of Drug ordered", default=None)
    drug_name: Optional[str] = Field(alias="Drug Name", default=None)
    effective_date: Optional[str] = Field(alias="Effective Date", default=None)
    dosage: Optional[str] = Field(alias="Dosage", default=None)
    drug_strength: Optional[str] = Field(alias="Drug Strength", default=None)
    frequency: Optional[str] = Field(alias="Frequency", default=None)
    duration: Optional[str] = Field(alias="Duration", default=None)
    route: Optional[str] = Field(alias="Route", default=None)
    number_of_refills_issued: Optional[str] = Field(alias="Number of Refills Issued", default=None)
    expiration_date: Optional[str] = Field(alias="Expiration Date", default=None)
    provider_admin_instructions: Optional[str] = Field(alias="Provider Admin Instructions", default=None)
    dispensed_quantity: Optional[str] = Field(alias="Dispensed Quantity", default=None)
    formulary_or_non_formulary_drug_flag: Optional[str] = Field(alias="Formulary Or Non Formulary Drug Flag", default=None)
    keep_on_person_indicator: Optional[str] = Field(alias="Keep On Person Indicator", default=None)
    medication_time_of_day: Optional[str] = Field(alias="What time of day the medication should be taken at (AM, PM, NOON, BEDTIME)", default=None)
    order_id: Optional[str] = Field(alias="ORDER_ID", default=None)
    ordered_by: Optional[str] = Field(alias="ORDERED_BY", default=None)

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 