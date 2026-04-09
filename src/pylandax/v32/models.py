from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class DocumentStatus(StrEnum):
    DRAFT = "draft"
    REVIEW = "review"
    APPROVAL = "approval"
    READY_TO_PUBLISH = "readytopublish"
    PUBLISHED = "published"
    HISTORY = "history"
    DISCARDED = "discarded"
    REVOKED = "revoked"


class DocumentType(StrEnum):
    FILE = "file"
    HYPERLINK = "hyperlink"
    WIKI = "wiki"


class OnDuplicateFile(StrEnum):
    IGNORE = "ignore"
    NEW_DRAFT = "newdraft"
    NEW_PUBLISHED = "newpublished"
    OVERWRITE = "overwrite"
    RENAME = "rename"


class GeographicData(BaseModel):
    AddressLine: str | None = None
    SecondaryAddressLine: str | None = None
    PostalCode: str | None = None
    City: str | None = None
    County: str | None = None
    Country: str | None = None
    MunicipalityNumber: int | None = None
    CadastralUnitNumber: int | None = None
    PropertyUnitNumber: int | None = None
    CondominiumUnitNumber: int | None = None
    Longitude: float | None = None
    Latitude: float | None = None
    PositionIsSetManuallyByUser: bool | None = None


class Incident(BaseModel):
    """Pydantic model for the Landax Incident entity (API v32)."""

    model_config = {"populate_by_name": True}

    Id: int | None = None
    Guid: str | None = None
    Number: str | None = None
    Subject: str | None = None
    Description: str | None = None
    OriginalHtmlDescription: str | None = None
    IsPrivate: bool | None = None
    IsDraft: bool | None = None
    Cause: str | None = None
    Signature: str | None = None
    ShowForAllUsers: bool | None = None
    ImmediateAction: str | None = None
    ImmediateActionConfirmedWorking: bool | None = None
    CorrectiveAction: str | None = None
    CorrectiveActionConfirmedWorking: bool | None = None
    CloseNotes: str | None = None
    CloseNotesConfirmedWorking: bool | None = None
    VerificationNotes: str | None = None

    # Dates
    IncidentDateTime: datetime | None = None
    DurationTo: datetime | None = None
    IsVerified: bool | None = None
    VerifiedDateTime: datetime | None = None
    PlannedCloseDate: datetime | None = None
    ClosedDate: datetime | None = None
    IsClosed: bool | None = None
    HandlingTime: float | None = None
    DurationTime: float | None = None

    # Coworker references
    ReporterCoworkerId: int | None = None
    InspectorCoworkerId: int | None = None
    ResponsibleFunctionId: int | None = None
    RegisteredByCoworkerId: int | None = None
    HandledByFunctionId: int | None = None
    HandledByCoworkerId: int | None = None
    VerifiedByCoworkerId: int | None = None
    ClosedByCoworkerId: int | None = None

    # Related entity references
    SupplierId: int | None = None
    ReportedBySupplierId: int | None = None
    TargetCoworkerId: int | None = None
    ContactPersonId: int | None = None
    ReportedByCustomerId: int | None = None
    CustomerId: int | None = None
    ContractId: int | None = None
    OccurenceDepartmentId: int | None = None
    DepartmentId: int | None = None
    ProjectId: int | None = None
    ProcessId: int | None = None
    ObservedInProcessId: int | None = None
    FlowchartObjectId: int | None = None
    OriginalFlowchartObjectId: int | None = None
    InspectionResponseId: int | None = None
    ProductId: int | None = None
    ProductIds: list[int] | None = None
    ProductQuantity: int | None = None
    ProductionDate: date | None = None
    PurchaseDate: date | None = None
    EquipmentIds: list[int] | None = None
    EquipmentBorrowId: int | None = None
    EquipmentGroupId: int | None = None

    # Audit references
    AuditObjectId: int | None = None
    AuditTypeId: int | None = None
    AuditId: int | None = None
    SurveyAnswerId: int | None = None
    PdaLogId: int | None = None

    # Location
    OccurenceLocationId: int | None = None
    LocationId: int | None = None
    TaskId: int | None = None

    # Classification
    TypeId: int | None = None
    WorkPositionId: int | None = None
    CategoryId: int | None = None
    IncidentSeverityId: int | None = None
    IncidentPriorityId: int | None = None
    IncidentFocusAreaId: int | None = None
    FocusAreaIds: list[int] | None = None
    InvolvedWorkPositionIds: list[int] | None = None
    IncidentStatusId: int | None = None

    # Metadata
    RegisteredDateTime: datetime | None = None
    ChangedDateTime: datetime | None = None
    ChangedByCoworkerId: int | None = None
    OperationLogEntryId: int | None = None
    AbsenseId: int | None = None

    # HACCP
    HaccpPlanId: int | None = None
    HaccpStepId: int | None = None
    HaccpHazardStepId: int | None = None
    PrerequisityId: int | None = None

    # Tags
    TagTypeIds: list[int] | None = None
    Tag: str | None = None
    Tag1: str | None = None
    Tag2: str | None = None
    Tag3: str | None = None
    Tag4: str | None = None
    Tag5: str | None = None
    Tag6: str | None = None
    Tag7: str | None = None
    Tag8: str | None = None
    Tag9: str | None = None
    Tag10: str | None = None
    Tag11: str | None = None
    Tag12: str | None = None

    # Tag references
    TagReference1Id: int | None = None
    TagReference2Id: int | None = None
    TagReference3Id: int | None = None
    TagReference4Id: int | None = None
    TagReference5Id: int | None = None
    TagReference6Ids: list[int] | None = None
    TagReference7Ids: list[int] | None = None
    TagReference8Ids: list[int] | None = None
    TagReference9Ids: list[int] | None = None
    TagReference10Ids: list[int] | None = None
    TagReference11Ids: list[int] | None = None
    TagReference12Ids: list[int] | None = None
    TagReference13Ids: list[int] | None = None
    TagReference14Ids: list[int] | None = None
    TagReference15Ids: list[int] | None = None

    # External
    ExternalId: str | None = None
    ExternalId1: int | None = None
    GroupIds: list[int] | None = None
    CauseIds: list[int] | None = None
    ComplianceResponseId: int | None = None

    # Economy
    SimpleEconomyText: str | None = None
    SimpleEconomyAmount: float | None = None
    ConvertedSimpleEconomyAmount: float | None = None
    ConvertedSimpleEconomyCurrency: str | None = None
    SimpleEconomyCurrency: str | None = None
    LineTotalWithoutVat: float | None = None

    # Miscellaneous
    DaysOfAbsence: int | None = None
    Downtime: int | None = None
    GeographicData_: GeographicData | None = Field(None, alias="GeographicData", serialization_alias="GeographicData")
    Longitude: float | None = None
    Latitude: float | None = None
    ChangedCoordinatesDateTime: datetime | None = None
    ChangedCoordinatesByCoworkerId: int | None = None
    LineTotalNoVat: float | None = None
    RegisteredByName: str | None = None
    RegisteredByEmail: str | None = None
    RegisteredByPhone: str | None = None
    RegisteredExternally: bool | None = None
    ObjectIds: list[int] | None = None
    CalculatedColor: str | None = None


class DocumentConfigDto(BaseModel):
    IsPositionFromExif: bool | None = None
    ImageWidth: int | None = None
    ImageHeight: int | None = None


class Document(BaseModel):
    """Pydantic model for the Landax Document entity (API v32)."""

    model_config = {"populate_by_name": True}

    Id: int | None = None
    DocumentId: int | None = None
    DocumentGuid: str | None = None
    VersionId: int | None = None
    Number: str | None = None
    VersionGuid: str | None = None
    ExternalId: str | None = None
    ExternalETag: str | None = None
    FolderId: int | None = None
    ModuleId: int | None = None
    VersionText: str | None = None
    Version: int | None = None
    Description: str | None = None
    TypeId: int | None = None
    CategoryId: int | None = None
    DocumentType_: DocumentType | None = Field(None, alias="DocumentType", serialization_alias="DocumentType")
    HyperLinkUrl: str | None = None
    FileId: int | None = None
    DepartmentIds: list[int] | None = None
    IsReadOnly: bool | None = None
    IsActive: bool | None = None
    IsStatic: bool | None = None
    IsTemplate: bool | None = None
    IsDraft: bool | None = None
    IsExport: bool | None = None
    IsKeyDocument: bool | None = None
    IsPublished: bool | None = None
    PublishedDateTime: datetime | None = None
    PublishedByCoworkerId: int | None = None
    IsHistory: bool | None = None
    IsArchieved: bool | None = None
    IsMainVersion: bool | None = None
    IsDirectLink: bool | None = None
    DatePictureTaken: datetime | None = None
    ShowInApp: bool | None = None
    OfflineInApp: bool | None = None
    Longitude: float | None = None
    Latitude: float | None = None
    Status_: DocumentStatus | None = Field(None, alias="Status", serialization_alias="Status")
    Notes: str | None = None
    DurationDate: date | None = None
    RevisedDateTime: date | None = None
    ReviewDeadline: date | None = None
    ApprovalDeadline: date | None = None
    ReadDeadline: date | None = None
    IsReviewed: bool | None = None
    ReviewDoneDateTime: datetime | None = None
    ReviewDoneByCoworkerId: int | None = None
    ReviewStartedDateTime: datetime | None = None
    ReviewStartedByCoworkerId: int | None = None
    ApprovalResponsibleCoworkerId: int | None = None
    IsApproved: bool | None = None
    ApprovalStartedByCoworkerId: int | None = None
    ApprovalStartedDateTime: datetime | None = None
    ApprovedByCoworkerId: int | None = None
    ApprovedDateTime: datetime | None = None
    DraftDateTime: datetime | None = None
    DraftByCoworkerId: int | None = None
    DeactivatedByCoworkerId: int | None = None
    ProcessIds: list[int] | None = None
    LocationIds: list[int] | None = None
    ProductIds: list[int] | None = None
    EquipmentIds: list[int] | None = None
    SupplierIds: list[int] | None = None
    CustomerIds: list[int] | None = None
    TagTypeIds: list[int] | None = None
    ResponsibleCoworkerId: int | None = None
    ReviewCoworkerIds: list[int] | None = None
    CoAuthorCoworkerIds: list[int] | None = None
    UseMerging: bool | None = None
    DownloadAsPdf: bool | None = None
    ControllerCoworkerId: int | None = None
    TagReference1Ids: list[int] | None = None
    TagReference2Ids: list[int] | None = None
    TagReference3Ids: list[int] | None = None
    TagReference4Ids: list[int] | None = None
    TagReference5Ids: list[int] | None = None
    TagReference6Ids: list[int] | None = None
    TagReference7Ids: list[int] | None = None
    TagReference8Ids: list[int] | None = None
    TagReference9Ids: list[int] | None = None
    TagReference10Ids: list[int] | None = None
    Config_: DocumentConfigDto | None = Field(None, alias="Config", serialization_alias="Config")
    Tag1: str | None = None
    Tag2: str | None = None
    Tag3: str | None = None
    Tag4: str | None = None
    Tag5: str | None = None
    SourceDocumentId: int | None = None
    SourceVersionId: int | None = None
    ChangedDateTime: datetime | None = None
    RegisteredDateTime: datetime | None = None
    RegisteredByCoworkerId: int | None = None
    ChangedByCoworkerId: int | None = None
    IsPublic: bool | None = None
    PublicFromDate: date | None = None
    PublicToDate: date | None = None
    PublicGuid: str | None = None
    FileName: str | None = None
    FileSize: int | None = None
    IconUrl: str | None = None
    IsVirus: bool | None = None
    CountLinkedDocuments: int | None = None
    CountReferencesToDocuments: int | None = None
    CountNoteRecords: int | None = None
    CountVersions: int | None = None


class CreateDocumentDto(BaseModel):
    """DTO for Documents/CreateDocument (multipart upload)."""

    model_config = {"populate_by_name": True}

    ModuleId: int | None = None
    FolderId: int | None = None
    DocumentGuid: str | None = None
    VersionGuid: str | None = None
    Description: str | None = None
    DocumentType_: DocumentType | None = Field(None, alias="DocumentType", serialization_alias="DocumentType")
    ResponsibleCoworkerId: int | None = None
    Number: str | None = None
    TypeId: int | None = None
    CategoryId: int | None = None
    UseMerging: bool | None = None
    DownloadAsPdf: bool | None = None
    HyperLinkUrl: str | None = None
    IsActive: bool | None = None
    ExternalId: str | None = None
    Version: int | None = None
    VersionText: str | None = None
    IsStatic: bool | None = None
    IsTemplate: bool | None = None
    IsKeyDocument: bool | None = None
    DatePictureTaken: datetime | None = None
    ShowInApp: bool | None = None
    OfflineInApp: bool | None = None
    Status_: DocumentStatus | None = Field(None, alias="Status", serialization_alias="Status")
    PublicFromDate: date | None = None
    PublicToDate: date | None = None
    ClassificationId: int | None = None
    RevisedDateTime: datetime | None = None
    IsExport: bool | None = None
    DepartmentIds: list[int] | None = None
    ProcessIds: list[int] | None = None
    LocationIds: list[int] | None = None
    LocationFolderId: int | None = None
    ProductIds: list[int] | None = None
    EquipmentIds: list[int] | None = None
    SupplierIds: list[int] | None = None
    CustomerIds: list[int] | None = None
    TagTypeIds: list[int] | None = None
    ReviewCoworkerIds: list[int] | None = None
    CoAuthorCoworkerIds: list[int] | None = None
    ControllerCoworkerId: int | None = None
    OnDuplicateFile_: OnDuplicateFile | None = Field(None, alias="OnDuplicateFile", serialization_alias="OnDuplicateFile")


class CreatedDocumentDto(BaseModel):
    """Response from Documents/CreateDocument."""

    DocumentId: int | None = None
    DocumentGuid: str | None = None
    VersionId: int | None = None
    VersionGuid: str | None = None


class CreateDocumentWithLinkDto(BaseModel):
    """DTO for Documents/CreateDocumentWithLink (multipart upload linked to a model record)."""

    model_config = {"populate_by_name": True}

    # Link fields
    ModelName: str | None = None
    RecordId: int | None = None
    LinkFolderId: int | None = None
    DocumentationRequestId: int | None = None
    DocumentationResponseId: int | None = None
    LinkGuid: str | None = None
    ReplaceLinkId: int | None = None
    IsMainModelImage: bool | None = None

    # Document fields (same as CreateDocumentDto)
    DocumentGuid: str | None = None
    VersionGuid: str | None = None
    Description: str | None = None
    DocumentType_: DocumentType | None = Field(None, alias="DocumentType", serialization_alias="DocumentType")
    ResponsibleCoworkerId: int | None = None
    Number: str | None = None
    TypeId: int | None = None
    CategoryId: int | None = None
    UseMerging: bool | None = None
    DownloadAsPdf: bool | None = None
    HyperLinkUrl: str | None = None
    IsActive: bool | None = None
    ExternalId: str | None = None
    Version: int | None = None
    VersionText: str | None = None
    IsStatic: bool | None = None
    IsTemplate: bool | None = None
    IsKeyDocument: bool | None = None
    DatePictureTaken: datetime | None = None
    ShowInApp: bool | None = None
    OfflineInApp: bool | None = None
    Status_: DocumentStatus | None = Field(None, alias="Status", serialization_alias="Status")
    PublicFromDate: date | None = None
    PublicToDate: date | None = None
    ClassificationId: int | None = None
    RevisedDateTime: datetime | None = None
    IsExport: bool | None = None
    DepartmentIds: list[int] | None = None
    ProcessIds: list[int] | None = None
    LocationIds: list[int] | None = None
    LocationFolderId: int | None = None
    ProductIds: list[int] | None = None
    EquipmentIds: list[int] | None = None
    SupplierIds: list[int] | None = None
    CustomerIds: list[int] | None = None
    TagTypeIds: list[int] | None = None
    ReviewCoworkerIds: list[int] | None = None
    CoAuthorCoworkerIds: list[int] | None = None
    ControllerCoworkerId: int | None = None
    OnDuplicateFile_: OnDuplicateFile | None = Field(None, alias="OnDuplicateFile", serialization_alias="OnDuplicateFile")


class CreatedDocumentWithLinkDto(BaseModel):
    """Response from Documents/CreateDocumentWithLink."""

    DocumentId: int | None = None
    DocumentGuid: str | None = None
    VersionId: int | None = None
    VersionGuid: str | None = None
    DocumentLinkId: int | None = None
    DocumentLinkGuid: str | None = None
