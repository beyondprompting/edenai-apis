from io import BufferedReader
from json import JSONDecodeError
from typing import Dict, List, Sequence

import requests

from edenai_apis.features import ProviderInterface, OcrInterface
from edenai_apis.features.ocr.identity_parser.identity_parser_dataclass import (
    IdentityParserDataClass,
)
from edenai_apis.features.ocr.identity_parser.identity_parser_dataclass import (
    InfoCountry,
    ItemIdentityParserDataClass,
    format_date,
    get_info_country,
)
from edenai_apis.features.ocr.identity_parser.identity_parser_dataclass import (
    InfosIdentityParserDataClass,
)
from edenai_apis.features.ocr.invoice_parser import InvoiceParserDataClass
from edenai_apis.features.ocr.invoice_parser.invoice_parser_dataclass import (
    BankInvoice,
    CustomerInformationInvoice,
    InfosInvoiceParserDataClass,
    ItemLinesInvoice,
    LocaleInvoice,
    MerchantInformationInvoice,
    TaxesInvoice,
)
from edenai_apis.features.ocr.receipt_parser import ReceiptParserDataClass
from edenai_apis.features.ocr.receipt_parser.receipt_parser_dataclass import (
    CustomerInformation,
    InfosReceiptParserDataClass,
    ItemLines,
    Locale,
    MerchantInformation,
    PaymentInformation,
    Taxes,
)
from edenai_apis.features.ocr.resume_parser import (
    ResumeParserDataClass,
    ResumeLocation,
    ResumeSkill,
    ResumeWorkExpEntry,
    ResumeWorkExp,
    ResumeEducationEntry,
    ResumeEducation,
    ResumePersonalName,
    ResumePersonalInfo,
    ResumeExtractedData,
)
from edenai_apis.loaders.loaders import load_provider, ProviderDataEnum
from edenai_apis.utils.exception import ProviderException
from edenai_apis.utils.parsing import extract
from edenai_apis.utils.types import ResponseType


class KlippaApi(ProviderInterface, OcrInterface):
    provider_name = "klippa"

    def __init__(self, api_keys: Dict = {}):
        self.api_settings = load_provider(
            ProviderDataEnum.KEY, self.provider_name, api_keys=api_keys
        )
        self.api_key = self.api_settings["subscription_key"]
        self.url = "https://custom-ocr.klippa.com/api/v1/parseDocument"

        self.headers = {
            "X-Auth-Key": self.api_key,
        }

    def _make_post_request(self, file: BufferedReader, endpoint: str = ""):
        files = {
            "document": file,
        }
        data = {"pdf_text_extraction": "full"}
        response = requests.post(
            url=self.url + endpoint, headers=self.headers, files=files, data=data
        )

        try:
            original_response = response.json()
        except JSONDecodeError:
            raise ProviderException(
                message="Internal Server Error", code=500
            ) from JSONDecodeError

        if response.status_code != 200:
            raise ProviderException(message=response.json(), code=response.status_code)

        return original_response

    def ocr__invoice_parser(
        self, file: str, language: str, file_url: str = ""
    ) -> ResponseType[InvoiceParserDataClass]:
        file_ = open(file, "rb")
        original_response = self._make_post_request(file_)

        file_.close()

        data_response = original_response["data"]
        customer_information = CustomerInformationInvoice(
            customer_name=data_response["customer_name"],
            customer_address=data_response["customer_address"],
            customer_email=data_response["customer_email"],
            customer_phone=data_response["customer_phone"],
            customer_tax_id=data_response["customer_vat_number"],
            customer_id=data_response["customer_id"],
            customer_billing_address=data_response["customer_address"],
            customer_mailing_address=None,
            customer_remittance_address=None,
            customer_service_address=None,
            customer_shipping_address=None,
            abn_number=None,
            vat_number=None,
            gst_number=None,
            pan_number=None,
        )

        merchant_information = MerchantInformationInvoice(
            merchant_name=data_response["merchant_name"],
            merchant_address=data_response["merchant_address"],
            merchant_email=data_response["merchant_email"],
            merchant_phone=data_response["merchant_phone"],
            merchant_tax_id=data_response["merchant_vat_number"],
            merchant_id=data_response["merchant_id"],
            merchant_siret=data_response["merchant_coc_number"],
            merchant_website=data_response["merchant_website"],
            merchant_fax=None,
            merchant_siren=None,
            abn_number=None,
            gst_number=None,
            pan_number=None,
            vat_number=None,
        )

        bank_information = BankInvoice(
            account_number=data_response["merchant_bank_account_number"],
            iban=data_response["merchant_bank_account_number_bic"],
            bsb=None,
            sort_code=data_response["merchant_bank_domestic_bank_code"],
            vat_number=None,
            rooting_number=None,
            swift=None,
        )

        tax_information = TaxesInvoice(
            value=data_response["personal_income_tax_amount"],
            rate=data_response["personal_income_tax_rate"],
        )

        locale_information = LocaleInvoice(
            currency=data_response["currency"],
            language=data_response["document_language"],
        )

        item_lines: List[ItemLinesInvoice] = []
        for line in data_response.get("lines", []):
            for item in line.get("lineitems", []):
                item_lines.append(
                    ItemLinesInvoice(
                        description=item["description"],
                        quantity=item["quantity"],
                        unit_price=item["amount_each"],
                        discount=item["discount_amount"],
                        amount=item["amount"],
                        tax_rate=item["vat_percentage"],
                        tax_amount=item["vat_amount"],
                        product_code=item["sku"],
                    )
                )

        standardize_response = InvoiceParserDataClass(
            extracted_data=[
                InfosInvoiceParserDataClass(
                    customer_information=customer_information,
                    merchant_information=merchant_information,
                    bank_informations=bank_information,
                    taxes=[tax_information],
                    locale=locale_information,
                    item_lines=item_lines,
                    invoice_number=data_response["invoice_number"],
                    invoice_date=data_response["date"],
                    invoice_total=data_response["amount"],
                )
            ]
        )

        return ResponseType[InvoiceParserDataClass](
            original_response=original_response,
            standardized_response=standardize_response,
        )

    def ocr__receipt_parser(
        self, file: str, language: str, file_url: str = ""
    ) -> ResponseType[ReceiptParserDataClass]:
        file_ = open(file, "rb")
        original_response = self._make_post_request(file_)

        file_.close()

        data_response = original_response["data"]
        customer_information = CustomerInformation(
            customer_name=data_response["customer_name"],
        )

        merchant_information = MerchantInformation(
            merchant_name=data_response["merchant_name"],
            merchant_address=data_response["merchant_address"],
            merchant_phone=data_response["merchant_phone"],
            merchant_tax_id=data_response["merchant_vat_number"],
            merchant_siret=data_response["merchant_coc_number"],
            merchant_url=data_response["merchant_website"],
        )

        locale_information = Locale(
            currency=data_response["currency"],
            language=data_response["document_language"],
            country=data_response["merchant_country_code"],
        )

        taxes_information = Taxes(
            rate=data_response["personal_income_tax_rate"],
            taxes=data_response["personal_income_tax_amount"],
        )

        payment_information = PaymentInformation(
            card_type=data_response["paymentmethod"],
            card_number=data_response["payment_card_number"],
        )

        item_lines: List[ItemLines] = []
        for line in data_response.get("lines", []):
            for lineitem in line.get("linetimes", []):
                item_lines.append(
                    ItemLines(
                        description=lineitem["description"],
                        quantity=lineitem["quantity"],
                        unit_price=lineitem["amount_each"],
                        amount=lineitem["amount"],
                    )
                )

        info_receipt = [
            InfosReceiptParserDataClass(
                customer_information=customer_information,
                merchant_information=merchant_information,
                locale=locale_information,
                taxes=[taxes_information],
                payment_information=payment_information,
                invoice_number=data_response["invoice_number"],
                date=data_response["date"],
                invoice_total=data_response["amount"],
            )
        ]

        standardize_response = ReceiptParserDataClass(extracted_data=info_receipt)

        return ResponseType[ReceiptParserDataClass](
            original_response=original_response,
            standardized_response=standardize_response,
        )

    def ocr__identity_parser(
        self, file: str, file_url: str = ""
    ) -> ResponseType[IdentityParserDataClass]:
        file_ = open(file, "rb")
        original_response = self._make_post_request(file_, endpoint="/identity")
        file_.close()

        items: Sequence[InfosIdentityParserDataClass] = []

        parsed_data = original_response.get("data", {}).get("parsed", {})

        issuing_country = get_info_country(
            key=InfoCountry.ALPHA3,
            value=(parsed_data.get("issuing_country") or {}).get("value", ""),
        )

        given_names_dict = parsed_data.get("given_names", {}) or {}
        given_names_string = given_names_dict.get("value", "") or ""
        given_names = given_names_string.split(" ")
        final_given_names = []
        for given_name in given_names:
            final_given_names.append(
                ItemIdentityParserDataClass(
                    value=given_name,
                    confidence=(parsed_data.get("given_names", {}) or {}).get(
                        "confidence"
                    ),
                )
            )
        birth_date = parsed_data.get("date_of_birth", {}) or {}
        birth_date_value = birth_date.get("value")
        birth_date_confidence = birth_date.get("confidence")
        formatted_birth_date = format_date(birth_date_value)

        issuance_date = parsed_data.get("date_of_issue", {}) or {}
        issuance_date_value = issuance_date.get("value")
        issuance_date_confidence = issuance_date.get("confidence")
        formatted_issuance_date = format_date(issuance_date_value)

        expire_date = parsed_data.get("date_of_expiry", {}) or {}
        expire_date_value = expire_date.get("value")
        expire_date_confidence = expire_date.get("confidence")
        formatted_expire_date = format_date(expire_date_value)

        last_name = parsed_data.get("surname", {}) or {}
        birth_place = parsed_data.get("place_of_birth", {}) or {}
        document_id = parsed_data.get("document_number", {}) or {}
        issuing_state = parsed_data.get("issuing_institution", {}) or {}

        addr = (parsed_data.get("address", {}) or {}).get("value") or {}
        street = addr.get("house_number", "") or ""
        street += f" {addr.get('street_name', '') or ''}"
        city = addr.get("post_code", "") or ""
        city += f" {addr.get('city', '') or ''}"
        province = addr.get("province", "") or ""
        country = addr.get("country", "") or ""
        formatted_address = ", ".join(
            filter(
                lambda x: x,
                [street.strip(), city.strip(), province.strip(), country.strip()],
            )
        )

        age = parsed_data.get("age", {}) or {}
        document_type = parsed_data.get("document_type", {}) or {}
        gender = parsed_data.get("gender", {}) or {}
        mrz = parsed_data.get("mrz", {}) or {}
        nationality = parsed_data.get("nationality", {}) or {}

        images = []

        if img_value := (parsed_data.get("face", {}) or {}).get("value", ""):
            images.append(
                ItemIdentityParserDataClass(
                    value=img_value,
                    confidence=None,
                )
            )
        identity_imgs = parsed_data.get("identity_document", []) or []
        if len(identity_imgs) > 0:
            for identity_img in identity_imgs:
                if img_value := identity_img.get("image", ""):
                    images.append(
                        ItemIdentityParserDataClass(
                            value=img_value,
                            confidence=None,
                        )
                    )

        items.append(
            InfosIdentityParserDataClass(
                last_name=ItemIdentityParserDataClass(
                    value=last_name.get("value"),
                    confidence=last_name.get("confidence"),
                ),
                given_names=final_given_names,
                birth_place=ItemIdentityParserDataClass(
                    value=birth_place.get("value"),
                    confidence=birth_place.get("confidence"),
                ),
                birth_date=ItemIdentityParserDataClass(
                    value=formatted_birth_date,
                    confidence=birth_date_confidence,
                ),
                issuance_date=ItemIdentityParserDataClass(
                    value=formatted_issuance_date,
                    confidence=issuance_date_confidence,
                ),
                expire_date=ItemIdentityParserDataClass(
                    value=formatted_expire_date,
                    confidence=expire_date_confidence,
                ),
                document_id=ItemIdentityParserDataClass(
                    value=document_id.get("value"),
                    confidence=document_id.get("confidence"),
                ),
                issuing_state=ItemIdentityParserDataClass(
                    value=issuing_state.get("value"),
                    confidence=issuing_state.get("confidence"),
                ),
                address=ItemIdentityParserDataClass(
                    value=formatted_address,
                ),
                age=ItemIdentityParserDataClass(
                    value=age.get("value"),
                    confidence=age.get("confidence"),
                ),
                country=issuing_country,
                document_type=ItemIdentityParserDataClass(
                    value=document_type.get("value"),
                    confidence=document_type.get("confidence"),
                ),
                gender=ItemIdentityParserDataClass(
                    value=gender.get("value"),
                    confidence=gender.get("confidence"),
                ),
                image_id=images,
                image_signature=[],
                mrz=ItemIdentityParserDataClass(
                    value=mrz.get("value"),
                    confidence=mrz.get("confidence"),
                ),
                nationality=ItemIdentityParserDataClass(
                    value=nationality.get("value"),
                    confidence=nationality.get("confidence"),
                ),
            )
        )

        standardized_response = IdentityParserDataClass(extracted_data=items)

        return ResponseType[IdentityParserDataClass](
            original_response=original_response,
            standardized_response=standardized_response,
        )

    def ocr__resume_parser(
        self, file: str, file_url: str = ""
    ) -> ResponseType[ResumeParserDataClass]:
        file_ = open(file, "rb")
        original_response = self._make_post_request(file_, endpoint="/resume")
        file_.close()
        response_data = original_response.get("data", {}).get("parsed", {})
        applicant = response_data["applicant"]
        name = ResumePersonalName(
            raw_name=extract(applicant, ['name', 'value']),
            first_name=None,
            last_name=None,
            middle=None,
            title=None,
            prefix=None,
            sufix=None,
        )
        address = ResumeLocation(
            formatted_location=None,
            postal_code=None,
            region=None,
            country=extract(applicant, ['address', 'country', 'value']),
            country_code=None,
            raw_input_location=None,
            street=None,
            street_number=None,
            appartment_number=None,
            city=extract(applicant, ['address', 'city', 'value']),
        )
        phones = extract(applicant, ['phone_number', 'value'])
        mails = extract(applicant, ['email_address', 'value'])
        urls = [website["value"] for website in applicant["websites"] if website.get("value")]
        personal_infos = ResumePersonalInfo(
            name=name,
            address=address,
            self_summary=None,
            objective=None,
            date_of_birth=None,
            place_of_birth=None,
            phones=[phones] if phones else [],
            mails=[mails] if mails else [],
            urls=urls,
            fax=[],
            current_profession=None,
            gender=None,
            nationality=None,
            martial_status=None,
            current_salary=None,
        )
        education_entries = []
        for edu in response_data.get("education", []):
            education_address = ResumeLocation(
                formatted_location=None,
                postal_code=None,
                region=None,
                country=extract(edu, ['address', 'country', 'value']),
                country_code=None,
                raw_input_location=None,
                street=None,
                street_number=None,
                appartment_number=None,
                city=extract(edu, ['address', 'city', 'value']),
            )
            education_entries.append(
                ResumeEducationEntry(
                    title=extract(edu, ['program', 'value']),
                    start_date=extract(edu, ['start', 'value']),
                    end_date=extract(edu, ['end', 'value']),
                    location=education_address,
                    establishment=extract(edu, ['institution', 'value']),
                    description=extract(edu, ['program', 'value']),
                    gpa=None,
                    accreditation=None,
                )
            )
        education = ResumeEducation(
            total_years_education=None,
            entries=education_entries,
        )
        work_experience_entries = []
        for work in response_data.get("work_experience", []):
            work_address = ResumeLocation(
                formatted_location=None,
                postal_code=None,
                region=None,
                country=extract(work, ['address', 'country', 'value']),
                country_code=None,
                raw_input_location=None,
                street=None,
                street_number=None,
                appartment_number=None,
                city=extract(work, ['address', 'city', 'value']),
            )
            work_experience_entries.append(
                ResumeWorkExpEntry(
                    title=extract(work, ['job_title', 'value']),
                    start_date=extract(work, ['start', 'value']),
                    end_date=extract(work, ['end', 'value']),
                    company=extract(work, ['company_name', 'value']),
                    location=work_address,
                    description=None,
                    industry=None,
                )
            )
        work_experience = ResumeWorkExp(
            total_years_experience=None,
            entries=work_experience_entries,
        )
        interests = []
        for interest in response_data.get("other_interests", []):
            interests.append(ResumeSkill(name=interest.get('value'), type=None))
        extracted_data = ResumeExtractedData(
            personal_infos=personal_infos,
            education=education,
            work_experience=work_experience,
            languages=[],
            skills=[],
            certifications=[],
            courses=[],
            publications=[],
            interests=interests,
        )
        standardized_response = ResumeParserDataClass(extracted_data=extracted_data)
        return ResponseType[ResumeParserDataClass](
            original_response=original_response,
            standardized_response=standardized_response,
        )
