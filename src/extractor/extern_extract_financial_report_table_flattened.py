"""
재무제표 XML 파서 - 외부 사용 인터페이스
입력: XML 파일 경로
출력: 추출된 텍스트 문자열
"""

from extract_financial_report_table_flattened import parse_full_document


def extract(file_path: str) -> str:
    """
    XML 재무제표 파일에서 텍스트를 추출합니다.

    Args:
        file_path: XML 파일 경로

    Returns:
        추출된 텍스트 문자열
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        xml_content = f.read()

    return parse_full_document(xml_content)
