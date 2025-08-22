from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date


class ActiveOrdersWithRxNorm(BaseModel):
    """Model for Active Orders with RxNorm data from incoming_*_ActiveORDERSWithRXNorm.csv"""
    facility_id: str = Field(alias="FACILITY_ID")
    unit: str = Field(alias="UNIT")
    npi_number_of_ordering_practitioner: str = Field(alias="NPI Number of ordering Practitioner")
    name_of_ordering_provider: str = Field(alias="Name of Ordering Provider")
    date_time_order_received: datetime = Field(alias="Date & Time order Received")
    ndc_code_of_drug_ordered: str = Field(alias="NDC Code of Drug ordered")
    drug_name: str = Field(alias="Drug Name")
    effective_date: date = Field(alias="Effective Date")
    dosage: float = Field(alias="Dosage")
    drug_strength: str = Field(alias="Drug Strength")
    frequency: str = Field(alias="Frequency")
    duration: int = Field(alias="Duration")
    route: str = Field(alias="Route")
    number_of_refills_issued: int = Field(alias="Number of Refills Issued")
    expiration_date: date = Field(alias="Expiration Date")
    provider_admin_instructions: str = Field(alias="Provider Admin Instructions")
    dispensed_quantity: float = Field(alias="Dispensed Quantity")
    formulary_or_non_formulary_drug_flag: str = Field(alias="Formulary Or Non Formulary Drug Flag")
    keep_on_person_indicator: str = Field(alias="Keep On Person Indicator")
    medication_time_of_day: str = Field(alias="What time of day the medication should be taken at (AM, PM, NOON, BEDTIME)")
    order_id: int = Field(alias="ORDER_ID")
    ordered_by: str = Field(alias="ORDERED_BY")

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True 