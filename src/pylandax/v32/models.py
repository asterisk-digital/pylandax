from __future__ import annotations

from datetime import date, datetime
from pydantic import BaseModel, Field


class GeographicData(BaseModel):
    address_line: str | None = Field(None, alias="AddressLine")
    secondary_address_line: str | None = Field(None, alias="SecondaryAddressLine")
    postal_code: str | None = Field(None, alias="PostalCode")
    city: str | None = Field(None, alias="City")
    county: str | None = Field(None, alias="County")
    country: str | None = Field(None, alias="Country")
    municipality_number: int | None = Field(None, alias="MunicipalityNumber")
    cadastral_unit_number: int | None = Field(None, alias="CadastralUnitNumber")
    property_unit_number: int | None = Field(None, alias="PropertyUnitNumber")
    condominium_unit_number: int | None = Field(None, alias="CondominiumUnitNumber")
    longitude: float | None = Field(None, alias="Longitude")
    latitude: float | None = Field(None, alias="Latitude")
    position_is_set_manually_by_user: bool | None = Field(None, alias="PositionIsSetManuallyByUser")

    model_config = {"populate_by_name": True}


class Incident(BaseModel):
    """Pydantic model for the Landax Incident entity (API v32)."""

    id: int | None = Field(None, alias="Id")
    guid: str | None = Field(None, alias="Guid")
    number: str | None = Field(None, alias="Number")
    subject: str | None = Field(None, alias="Subject")
    description: str | None = Field(None, alias="Description")
    original_html_description: str | None = Field(None, alias="OriginalHtmlDescription")
    is_private: bool | None = Field(None, alias="IsPrivate")
    is_draft: bool | None = Field(None, alias="IsDraft")
    cause: str | None = Field(None, alias="Cause")
    signature: str | None = Field(None, alias="Signature")
    show_for_all_users: bool | None = Field(None, alias="ShowForAllUsers")
    immediate_action: str | None = Field(None, alias="ImmediateAction")
    immediate_action_confirmed_working: bool | None = Field(None, alias="ImmediateActionConfirmedWorking")
    corrective_action: str | None = Field(None, alias="CorrectiveAction")
    corrective_action_confirmed_working: bool | None = Field(None, alias="CorrectiveActionConfirmedWorking")
    close_notes: str | None = Field(None, alias="CloseNotes")
    close_notes_confirmed_working: bool | None = Field(None, alias="CloseNotesConfirmedWorking")
    verification_notes: str | None = Field(None, alias="VerificationNotes")

    # Dates
    incident_date_time: datetime | None = Field(None, alias="IncidentDateTime")
    duration_to: datetime | None = Field(None, alias="DurationTo")
    is_verified: bool | None = Field(None, alias="IsVerified")
    verified_date_time: datetime | None = Field(None, alias="VerifiedDateTime")
    planned_close_date: datetime | None = Field(None, alias="PlannedCloseDate")
    closed_date: datetime | None = Field(None, alias="ClosedDate")
    is_closed: bool | None = Field(None, alias="IsClosed")
    handling_time: float | None = Field(None, alias="HandlingTime")
    duration_time: float | None = Field(None, alias="DurationTime")

    # Coworker references
    reporter_coworker_id: int | None = Field(None, alias="ReporterCoworkerId")
    inspector_coworker_id: int | None = Field(None, alias="InspectorCoworkerId")
    responsible_function_id: int | None = Field(None, alias="ResponsibleFunctionId")
    registered_by_coworker_id: int | None = Field(None, alias="RegisteredByCoworkerId")
    handled_by_function_id: int | None = Field(None, alias="HandledByFunctionId")
    handled_by_coworker_id: int | None = Field(None, alias="HandledByCoworkerId")
    verified_by_coworker_id: int | None = Field(None, alias="VerifiedByCoworkerId")
    closed_by_coworker_id: int | None = Field(None, alias="ClosedByCoworkerId")

    # Related entity references
    supplier_id: int | None = Field(None, alias="SupplierId")
    reported_by_supplier_id: int | None = Field(None, alias="ReportedBySupplierId")
    target_coworker_id: int | None = Field(None, alias="TargetCoworkerId")
    contact_person_id: int | None = Field(None, alias="ContactPersonId")
    reported_by_customer_id: int | None = Field(None, alias="ReportedByCustomerId")
    customer_id: int | None = Field(None, alias="CustomerId")
    contract_id: int | None = Field(None, alias="ContractId")
    occurence_department_id: int | None = Field(None, alias="OccurenceDepartmentId")
    department_id: int | None = Field(None, alias="DepartmentId")
    project_id: int | None = Field(None, alias="ProjectId")
    process_id: int | None = Field(None, alias="ProcessId")
    observed_in_process_id: int | None = Field(None, alias="ObservedInProcessId")
    flowchart_object_id: int | None = Field(None, alias="FlowchartObjectId")
    original_flowchart_object_id: int | None = Field(None, alias="OriginalFlowchartObjectId")
    inspection_response_id: int | None = Field(None, alias="InspectionResponseId")
    product_id: int | None = Field(None, alias="ProductId")
    product_ids: list[int] | None = Field(None, alias="ProductIds")
    product_quantity: int | None = Field(None, alias="ProductQuantity")
    production_date: date | None = Field(None, alias="ProductionDate")
    purchase_date: date | None = Field(None, alias="PurchaseDate")
    equipment_ids: list[int] | None = Field(None, alias="EquipmentIds")
    equipment_borrow_id: int | None = Field(None, alias="EquipmentBorrowId")
    equipment_group_id: int | None = Field(None, alias="EquipmentGroupId")

    # Audit references
    audit_object_id: int | None = Field(None, alias="AuditObjectId")
    audit_type_id: int | None = Field(None, alias="AuditTypeId")
    audit_id: int | None = Field(None, alias="AuditId")
    survey_answer_id: int | None = Field(None, alias="SurveyAnswerId")
    pda_log_id: int | None = Field(None, alias="PdaLogId")

    # Location
    occurence_location_id: int | None = Field(None, alias="OccurenceLocationId")
    location_id: int | None = Field(None, alias="LocationId")
    task_id: int | None = Field(None, alias="TaskId")

    # Classification
    type_id: int | None = Field(None, alias="TypeId")
    work_position_id: int | None = Field(None, alias="WorkPositionId")
    category_id: int | None = Field(None, alias="CategoryId")
    incident_severity_id: int | None = Field(None, alias="IncidentSeverityId")
    incident_priority_id: int | None = Field(None, alias="IncidentPriorityId")
    incident_focus_area_id: int | None = Field(None, alias="IncidentFocusAreaId")
    focus_area_ids: list[int] | None = Field(None, alias="FocusAreaIds")
    involved_work_position_ids: list[int] | None = Field(None, alias="InvolvedWorkPositionIds")
    incident_status_id: int | None = Field(None, alias="IncidentStatusId")

    # Metadata
    registered_date_time: datetime | None = Field(None, alias="RegisteredDateTime")
    changed_date_time: datetime | None = Field(None, alias="ChangedDateTime")
    changed_by_coworker_id: int | None = Field(None, alias="ChangedByCoworkerId")
    operation_log_entry_id: int | None = Field(None, alias="OperationLogEntryId")
    absense_id: int | None = Field(None, alias="AbsenseId")

    # HACCP
    haccp_plan_id: int | None = Field(None, alias="HaccpPlanId")
    haccp_step_id: int | None = Field(None, alias="HaccpStepId")
    haccp_hazard_step_id: int | None = Field(None, alias="HaccpHazardStepId")
    prerequisity_id: int | None = Field(None, alias="PrerequisityId")

    # Tags
    tag_type_ids: list[int] | None = Field(None, alias="TagTypeIds")
    tag: str | None = Field(None, alias="Tag")
    tag1: str | None = Field(None, alias="Tag1")
    tag2: str | None = Field(None, alias="Tag2")
    tag3: str | None = Field(None, alias="Tag3")
    tag4: str | None = Field(None, alias="Tag4")
    tag5: str | None = Field(None, alias="Tag5")
    tag6: str | None = Field(None, alias="Tag6")
    tag7: str | None = Field(None, alias="Tag7")
    tag8: str | None = Field(None, alias="Tag8")
    tag9: str | None = Field(None, alias="Tag9")
    tag10: str | None = Field(None, alias="Tag10")
    tag11: str | None = Field(None, alias="Tag11")
    tag12: str | None = Field(None, alias="Tag12")

    # Tag references
    tag_reference1_id: int | None = Field(None, alias="TagReference1Id")
    tag_reference2_id: int | None = Field(None, alias="TagReference2Id")
    tag_reference3_id: int | None = Field(None, alias="TagReference3Id")
    tag_reference4_id: int | None = Field(None, alias="TagReference4Id")
    tag_reference5_id: int | None = Field(None, alias="TagReference5Id")
    tag_reference6_ids: list[int] | None = Field(None, alias="TagReference6Ids")
    tag_reference7_ids: list[int] | None = Field(None, alias="TagReference7Ids")
    tag_reference8_ids: list[int] | None = Field(None, alias="TagReference8Ids")
    tag_reference9_ids: list[int] | None = Field(None, alias="TagReference9Ids")
    tag_reference10_ids: list[int] | None = Field(None, alias="TagReference10Ids")
    tag_reference11_ids: list[int] | None = Field(None, alias="TagReference11Ids")
    tag_reference12_ids: list[int] | None = Field(None, alias="TagReference12Ids")
    tag_reference13_ids: list[int] | None = Field(None, alias="TagReference13Ids")
    tag_reference14_ids: list[int] | None = Field(None, alias="TagReference14Ids")
    tag_reference15_ids: list[int] | None = Field(None, alias="TagReference15Ids")

    # External
    external_id: str | None = Field(None, alias="ExternalId")
    external_id1: int | None = Field(None, alias="ExternalId1")
    group_ids: list[int] | None = Field(None, alias="GroupIds")
    cause_ids: list[int] | None = Field(None, alias="CauseIds")
    compliance_response_id: int | None = Field(None, alias="ComplianceResponseId")

    # Economy
    simple_economy_text: str | None = Field(None, alias="SimpleEconomyText")
    simple_economy_amount: float | None = Field(None, alias="SimpleEconomyAmount")
    converted_simple_economy_amount: float | None = Field(None, alias="ConvertedSimpleEconomyAmount")
    converted_simple_economy_currency: str | None = Field(None, alias="ConvertedSimpleEconomyCurrency")
    simple_economy_currency: str | None = Field(None, alias="SimpleEconomyCurrency")
    line_total_without_vat: float | None = Field(None, alias="LineTotalWithoutVat")

    # Miscellaneous
    days_of_absence: int | None = Field(None, alias="DaysOfAbsence")
    downtime: int | None = Field(None, alias="Downtime")
    geographic_data: GeographicData | None = Field(None, alias="GeographicData")
    longitude: float | None = Field(None, alias="Longitude")
    latitude: float | None = Field(None, alias="Latitude")
    changed_coordinates_date_time: datetime | None = Field(None, alias="ChangedCoordinatesDateTime")
    changed_coordinates_by_coworker_id: int | None = Field(None, alias="ChangedCoordinatesByCoworkerId")
    line_total_no_vat: float | None = Field(None, alias="LineTotalNoVat")
    registered_by_name: str | None = Field(None, alias="RegisteredByName")
    registered_by_email: str | None = Field(None, alias="RegisteredByEmail")
    registered_by_phone: str | None = Field(None, alias="RegisteredByPhone")
    registered_externally: bool | None = Field(None, alias="RegisteredExternally")
    object_ids: list[int] | None = Field(None, alias="ObjectIds")
    calculated_color: str | None = Field(None, alias="CalculatedColor")

    model_config = {"populate_by_name": True}
