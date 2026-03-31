"""
재무제표 전체 SECTION 파서
XML 파일에서 모든 SECTION을 계층 구조에 따라 파싱하여 일반 문서 형식으로 변환합니다.

기능:
- SECTION-1, SECTION-2, SECTION-3 등 계층 구조 재귀 처리
- 각 SECTION별로 목차 생성
- <P> 태그: 텍스트 내용 그대로 출력
- <TABLE> 태그: RAG/KG 구축에 용이한 텍스트 포맷으로 변환
- 테이블 태깅에 테이블명 포함: [[테이블_시작]]_테이블명

사용법:
    python3 parse_all_section_claude.py [입력파일] [출력디렉토리]

예시:
    python3 parse_all_section_claude.py input.xml ./output
"""

from bs4 import BeautifulSoup, NavigableString
import re
import os
from pathlib import Path


def clean_text(text):
    """텍스트 정리: 여러 공백을 하나로, 앞뒤 공백 제거, &cr 제거"""
    if not text:
        return ""
    # &cr 제거
    text = text.replace('&cr', '')
    # 여러 공백을 하나로
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def get_table_name_from_prev_p(table_group):
    """
    TABLE-GROUP 앞의 P 태그에서 테이블명 추출

    우선순위:
    1. 바로 앞 P 태그 텍스트 (※로 시작하지 않고, 의미있는 경우)
    2. 부모 SECTION의 TITLE
    3. None (빈값)
    """
    # 바로 앞 P 태그 찾기
    prev_sibling = table_group.find_previous_sibling()
    while prev_sibling:
        if hasattr(prev_sibling, 'name') and prev_sibling.name == 'P':
            prev_text = clean_text(prev_sibling.get_text())

            # 의미없는 텍스트 스킵
            if not prev_text or len(prev_text) < 3:
                prev_sibling = prev_sibling.find_previous_sibling()
                continue

            # ※로 시작하는 주석은 스킵하고 그 앞 P 태그 찾기
            if prev_text.startswith('※'):
                prev_sibling = prev_sibling.find_previous_sibling()
                continue

            # "(제 XX기)" 형식은 스킵
            if re.match(r'^\(제\s*\d+\s*기\)$', prev_text):
                prev_sibling = prev_sibling.find_previous_sibling()
                continue

            # 테이블명 정제
            table_name = prev_text

            # "가. 제목" 형식에서 제목 부분만 추출 (첫 문장)
            # 긴 설명은 제거 (두 번째 문장 이후)
            # 예: "가. 배당에 관한 사항 당 사 는..." -> "가. 배당에 관한 사항"

            # 항목 번호 패턴 확인: 가., 나., (1), 1), - 등
            item_pattern = re.match(r'^([가-힣]\.|\([0-9]+\)|[0-9]+\)|\-)\s*', table_name)
            if item_pattern:
                prefix = item_pattern.group(0)
                rest = table_name[len(prefix):]
                # 나머지에서 불필요한 긴 설명 제거
                for sep in [' 당 ', ' 회사의 ', '(단위', '  ']:
                    if sep in rest:
                        rest = rest.split(sep)[0]
                        break
                table_name = prefix + rest
            else:
                # 항목 번호 없는 경우
                for sep in ['. ', '(단위', '  ']:
                    if sep in table_name:
                        table_name = table_name.split(sep)[0]
                        break

            # 최대 50자로 제한
            return table_name.strip()[:50]

        prev_sibling = prev_sibling.find_previous_sibling()

    # P 태그에서 못 찾으면 부모 SECTION의 TITLE 사용
    parent = table_group.parent
    while parent:
        if parent.name and parent.name.upper().startswith('SECTION'):
            parent_title = parent.find('TITLE', recursive=False)
            if parent_title:
                return clean_text(parent_title.get_text())[:50]
            break
        parent = parent.parent

    return None


def parse_table_to_text(table, table_name=None):
    """
    테이블을 RAG/KG 구축에 용이한 텍스트 형식으로 변환
    빈 테이블은 스킵함

    Args:
        table: BeautifulSoup 테이블 요소
        table_name: 테이블명 (TABLE-GROUP의 TITLE ATOC="N" 값)
    """
    lines = []

    # 헤더 추출
    headers = []
    thead = table.find('THEAD')
    if thead:
        header_rows = thead.find_all('TR')
        if header_rows:
            last_header_row = header_rows[-1]
            for th in last_header_row.find_all(['TH', 'TD', 'TE', 'TU']):
                text = clean_text(th.get_text())
                if text and text != '':
                    headers.append(text)

    # 헤더가 없으면 tbody에서 첫 행을 헤더로 간주
    tbody = table.find('TBODY')
    if not headers and tbody:
        first_row = tbody.find('TR')
        if first_row:
            for td in first_row.find_all(['TD', 'TH', 'TE', 'TU']):
                text = clean_text(td.get_text())
                if text:
                    headers.append(text)

    # 본문 데이터 추출
    data_rows = []
    if tbody:
        rows = tbody.find_all('TR')
        start_idx = 1 if not thead and headers else 0

        for row in rows[start_idx:]:
            cells = row.find_all(['TD', 'TH', 'TE', 'TU'])
            row_data = []
            for cell in cells:
                text = clean_text(cell.get_text())
                row_data.append(text if text else "-")

            # 빈 행이 아닌 경우만 추가
            if row_data and any(cell != "-" for cell in row_data):
                data_rows.append(row_data)

    # 테이블이 비어있는지 확인 (헤더도 없고 데이터도 없으면 스킵)
    if not headers and not data_rows:
        return ""

    # 헤더만 있고 데이터가 없어도 스킵
    if headers and not data_rows:
        return ""

    # 테이블 출력 - 테이블명이 있으면 포함
    if table_name:
        lines.append(f"\n[[테이블_시작]]_{table_name}")
    else:
        lines.append("\n[[테이블_시작]]")

    # 단위 정보 처리
    if thead and len(thead.find_all('TR')) > 1:
        first_row = thead.find_all('TR')[0]
        first_row_text = clean_text(first_row.get_text())
        if '단위' in first_row_text:
            lines.append(first_row_text)

    # 헤더 출력
    if headers:
        lines.append(" | ".join(headers))
        lines.append("-" * min(len(" | ".join(headers)), 80))

    # 데이터 행 출력
    for row_data in data_rows:
        lines.append(" | ".join(row_data))

    lines.append("")

    #테이블 출력 끝
    lines.append("\n[[테이블_끝]]")
    return "\n".join(lines)


def parse_section_content(section_element):
    """
    SECTION 요소의 내용을 파싱 (재귀적으로 하위 SECTION 처리)
    """
    output_lines = []

    # TITLE 추출
    title_tag = section_element.find('TITLE', recursive=False)
    if title_tag:
        title = clean_text(title_tag.get_text())
        if title:
            # SECTION 레벨에 따라 다른 구분선 사용
            section_name = section_element.name.upper()
            if 'SECTION-1' in section_name:
                output_lines.append("\n" + "="*80)
                output_lines.append(title)
                output_lines.append("="*80 + "\n")
            elif 'SECTION-2' in section_name:
                output_lines.append("\n" + "-"*80)
                output_lines.append(title)
                output_lines.append("-"*80 + "\n")
            elif 'SECTION-3' in section_name:
                output_lines.append("\n" + "·"*80)
                output_lines.append(title)
                output_lines.append("·"*80 + "\n")
            else:
                output_lines.append(f"\n## {title}\n")

    # 직계 자식 요소만 처리 (하위 SECTION은 재귀로 처리)
    def process_element(element, current_table_name=None):
        """
        요소를 재귀적으로 처리하는 헬퍼 함수

        Args:
            element: 처리할 요소
            current_table_name: 현재 TABLE-GROUP의 테이블명 (TITLE ATOC="N" 값)
        """
        result_lines = []

        for child in element.children:
            if isinstance(child, NavigableString):
                continue

            child_name = child.name.upper() if child.name else ""

            # 컨테이너 태그는 내부 요소를 재귀 처리
            if child_name in ['LIBRARY', 'TABLE-GROUP']:
                # TABLE-GROUP의 TITLE 처리
                if child_name == 'TABLE-GROUP':
                    group_title = child.find('TITLE', recursive=False)
                    table_name_for_group = None

                    if group_title:
                        title_text = clean_text(group_title.get_text())
                        if title_text:
                            result_lines.append(f"\n[{title_text}]")
                            result_lines.append("")

                        # ATOC="N" 속성 확인
                        if group_title.get('ATOC') == 'N':
                            table_name_for_group = title_text
                    else:
                        # TITLE이 없는 경우: 앞 P 태그에서 테이블명 추출
                        table_name_for_group = get_table_name_from_prev_p(child)

                    # TABLE-GROUP 내부 처리 시 테이블명 전달
                    result_lines.extend(process_element(child, table_name_for_group))
                else:
                    # LIBRARY는 현재 테이블명 유지
                    result_lines.extend(process_element(child, current_table_name))

            # 하위 SECTION 발견 시 재귀 처리
            elif child_name.startswith('SECTION-'):
                subsection_content = parse_section_content(child)
                result_lines.append(subsection_content)

            # P 태그 처리
            elif child_name == 'P':
                text = clean_text(child.get_text())
                if text:
                    result_lines.append(text)
                    result_lines.append("")

            # TABLE 태그 처리 - 테이블명 전달
            elif child_name == 'TABLE':
                table_text = parse_table_to_text(child, current_table_name)
                # 빈 테이블이 아닌 경우만 추가
                if table_text:
                    result_lines.append(table_text)

            # IMAGE 태그 처리
            elif child_name == 'IMAGE':
                img_tag = child.find('IMG')
                caption_tag = child.find('IMG-CAPTION')
                if img_tag and img_tag.string:
                    result_lines.append(f"\n[이미지: {clean_text(img_tag.string)}]")
                if caption_tag:
                    result_lines.append(f"({clean_text(caption_tag.get_text())})")
                result_lines.append("")

        return result_lines

    output_lines.extend(process_element(section_element))
    return "\n".join(output_lines)


def generate_toc(soup):
    """
    전체 문서의 목차 생성 (SECTION + TABLE-GROUP 포함)
    """
    toc_lines = ["="*80, "목차", "="*80, ""]

    def add_section_to_toc(section, base_indent=0):
        """SECTION과 그 하위 요소들을 재귀적으로 목차에 추가"""
        section_name = section.name.upper()
        level = int(section_name.split('-')[1])
        indent = "  " * (level - 1 + base_indent)

        # SECTION의 TITLE
        title_tag = section.find('TITLE', recursive=False)
        if title_tag:
            title = clean_text(title_tag.get_text())
            if title:
                toc_lines.append(f"{indent}{title}")

        # 하위 요소 순회 (SECTION-2, SECTION-3, TABLE-GROUP 등)
        for child in section.children:
            if not hasattr(child, 'name') or not child.name:
                continue

            child_name = child.name.upper()

            # 하위 SECTION 재귀 처리
            if child_name.startswith('SECTION-'):
                add_section_to_toc(child, base_indent)

            # LIBRARY 내부 탐색
            elif child_name == 'LIBRARY':
                for lib_child in child.children:
                    if hasattr(lib_child, 'name') and lib_child.name:
                        lib_child_name = lib_child.name.upper()
                        if lib_child_name.startswith('SECTION-'):
                            add_section_to_toc(lib_child, base_indent)
                        elif lib_child_name == 'TABLE-GROUP':
                            group_title = lib_child.find('TITLE', recursive=False)
                            if group_title:
                                group_title_text = clean_text(group_title.get_text())
                                if group_title_text:
                                    group_indent = "  " * (level + base_indent)
                                    toc_lines.append(f"{group_indent}{group_title_text}")

            # TABLE-GROUP의 TITLE 추가
            elif child_name == 'TABLE-GROUP':
                group_title = child.find('TITLE', recursive=False)
                if group_title:
                    group_title_text = clean_text(group_title.get_text())
                    if group_title_text:
                        group_indent = "  " * (level + base_indent)
                        toc_lines.append(f"{group_indent}{group_title_text}")

    # 모든 최상위 SECTION 찾기
    body = soup.find('BODY')
    if body:
        for child in body.children:
            if hasattr(child, 'name') and child.name and re.match(r'^SECTION-\d+$', child.name, re.IGNORECASE):
                add_section_to_toc(child)

    toc_lines.append("")
    return "\n".join(toc_lines)


def parse_full_document(xml_content):
    """
    전체 XML 문서 파싱
    """
    # lxml-xml 파서 사용 (XML 대소문자 유지)
    soup = BeautifulSoup(xml_content, 'lxml-xml')

    output_lines = []

    # 문서 제목
    doc_name = soup.find('DOCUMENT-NAME')
    company_name = soup.find('COMPANY-NAME')

    if doc_name or company_name:
        output_lines.append("="*80)
        if company_name:
            output_lines.append(clean_text(company_name.get_text()))
        if doc_name:
            output_lines.append(clean_text(doc_name.get_text()))
        output_lines.append("="*80)
        output_lines.append("")

    # 목차 생성
    toc = generate_toc(soup)
    output_lines.append(toc)

    # 모든 SECTION-1 처리 (BODY 내 어디에 있든 찾음)
    sections = soup.find_all('SECTION-1')

    for section in sections:
        section_content = parse_section_content(section)
        output_lines.append(section_content)
        output_lines.append("\n")

    return "\n".join(output_lines)


def split_by_sections(xml_content):
    """
    각 SECTION-1을 개별 파일로 분리
    반환: {section_id: {title: str, content: str}}
    """
    soup = BeautifulSoup(xml_content, 'lxml-xml')
    sections_dict = {}

    # 모든 SECTION-1 찾기 (중첩 구조 지원)
    sections = soup.find_all('SECTION-1')
    if not sections:
        return sections_dict

    for idx, section in enumerate(sections, 1):
        title_tag = section.find('TITLE', recursive=False)
        title = clean_text(title_tag.get_text()) if title_tag else f"Section {idx}"

        # 안전한 파일명 생성
        safe_title = re.sub(r'[^\w가-힣\s\-]', '', title)[:50]
        section_id = f"section_{idx:02d}_{safe_title}"

        content = parse_section_content(section)

        sections_dict[section_id] = {
            'title': title,
            'content': content,
            'order': idx
        }

    return sections_dict


def main():
    import sys

    # 입력 파일 읽기
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = "/Users/koscom/Desktop/dev/MyNarrative/data/raw/financial_report/108860_셀바스AI_quarterly_2024Q1_20240513000632.xml"

    # 입력 파일명에서 .xml을 제외한 이름 추출
    input_filename = os.path.basename(input_file)
    dir_name = os.path.splitext(input_filename)[0]  # .xml 제거

    # 기본 출력 디렉토리는 dbs 하위에 파일명으로 생성
    base_output_dir = "/Users/koscom/Desktop/dev/MyNarrative/devplace/dbs"
    output_dir = os.path.join(base_output_dir, dir_name)

    # 사용자가 출력 디렉토리를 지정한 경우 우선 적용
    if len(sys.argv) > 2:
        output_dir = sys.argv[2]

    # 출력 디렉토리 생성
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    print(f"파일 읽는 중: {input_file}")
    print(f"출력 디렉토리: {output_dir}")

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()

        print("전체 문서 파싱 중...")
        full_content = parse_full_document(xml_content)

        # 회사명 추출하여 파일명 생성
        soup = BeautifulSoup(xml_content, 'lxml-xml')
        company_name = soup.find('COMPANY-NAME')
        doc_name = soup.find('DOCUMENT-NAME')

        # 파일명 생성
        filename_parts = []
        if company_name:
            name = clean_text(company_name.get_text())
            # 특수문자 제거
            name = re.sub(r'[^\w가-힣\s]', '', name).strip()
            filename_parts.append(name)
        if doc_name:
            doc = clean_text(doc_name.get_text())
            doc = re.sub(r'[^\w가-힣\s]', '', doc).strip()
            filename_parts.append(doc)

        if filename_parts:
            output_filename = "_".join(filename_parts) + "_full_text.txt"
        else:
            output_filename = "full_document.txt"

        # 전체 문서 저장
        full_output_file = os.path.join(output_dir, output_filename)
        with open(full_output_file, 'w', encoding='utf-8') as f:
            f.write(full_content)
        print(f"전체 문서 저장: {full_output_file}")

        print("\nSECTION별 분리 중...")
        sections = split_by_sections(xml_content)

        # 각 SECTION을 개별 파일로 저장
        for section_id, section_data in sections.items():
            section_file = os.path.join(output_dir, f"{section_id}.txt")
            with open(section_file, 'w', encoding='utf-8') as f:
                f.write(section_data['content'])
            print(f"  - {section_data['title']} → {section_id}.txt")

        # 목차 파일 생성
        index_file = os.path.join(output_dir, "index.txt")
        with open(index_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("섹션별 파일 목록\n")
            f.write("="*80 + "\n\n")

            for section_id, section_data in sorted(sections.items(), key=lambda x: x[1]['order']):
                file_path = os.path.join(output_dir, f"{section_id}.txt")
                file_size = os.path.getsize(file_path) / 1024  # KB
                f.write(f"{section_data['order']:2d}. {section_data['title']}\n")
                f.write(f"    파일: {section_id}.txt ({file_size:.1f} KB)\n\n")

        print(f"\n목차 파일 저장: {index_file}")
        print(f"\n완료! 총 {len(sections)}개 섹션 처리됨")
        print(f"출력 디렉토리: {output_dir}")

        # 통계 출력
        total_size = sum(os.path.getsize(os.path.join(output_dir, f"{sid}.txt"))
                        for sid in sections.keys()) / 1024
        print(f"총 파일 크기: {total_size:.1f} KB")

        full_size = os.path.getsize(full_output_file) / 1024
        print(f"전체 문서 크기: {full_size:.1f} KB")

    except FileNotFoundError:
        print(f"오류: 파일을 찾을 수 없습니다 - {input_file}")
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
