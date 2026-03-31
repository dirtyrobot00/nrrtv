"""
재무제표 전체 SECTION 파서 (RAG Chunking 최적화 버전)
XML 파일에서 모든 SECTION을 계층 구조에 따라 파싱하여 일반 문서 형식으로 변환합니다.

기능:
- SECTION-1, SECTION-2, SECTION-3 등 계층 구조 재귀 처리
- 각 SECTION별로 목차 생성
- <P> 태그: 텍스트 내용 그대로 출력
- <TABLE> 태그: 컬럼명+행명+값 형식으로 평탄화 (RAG용)
- Recursive Chunking: 최적의 청크 크기로 분할

사용법:
    python3 parse_all_section_claude_2.py [입력파일] [출력디렉토리]

예시:
    python3 parse_all_section_claude_2.py input.xml ./output
"""

from bs4 import BeautifulSoup, NavigableString
import re
import os
from pathlib import Path


# ============================================================================
# 청크 설정 (RAG 최적화)
# ============================================================================
# 논문 및 선례상 최적의 청크 크기: 512~1024 토큰 (한글 기준 약 300~600자)
CHUNK_SIZE = 500  # 목표 청크 크기 (문자 수)
CHUNK_OVERLAP = 50  # 청크 간 오버랩 (문맥 유지)
MIN_CHUNK_SIZE = 100  # 최소 청크 크기


def clean_text(text):
    """텍스트 정리: 여러 공백을 하나로, 앞뒤 공백 제거, &cr 제거"""
    if not text:
        return ""
    # &cr 제거
    text = text.replace('&cr', '')
    # 여러 공백을 하나로
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def compress_repeated_chars(text, max_repeat=3):
    """
    특수문자의 연속 반복을 최대 max_repeat개로 압축

    예시:
    "========" → "==="
    "--------" → "---"
    "········" → "···"

    Args:
        text: 처리할 텍스트
        max_repeat: 최대 반복 횟수 (기본값: 3)

    Returns:
        압축된 텍스트
    """
    if not text:
        return ""

    # 특수문자 패턴: 같은 특수문자가 3회 이상 연속되면 max_repeat개로 압축
    # \W는 단어 문자가 아닌 것 (특수문자, 공백 등)
    # 단, 공백과 줄바꿈은 제외
    special_chars = r'[=\-·*#@!$%^&()_+\[\]{}|\\:";\'<>?,./~`]'

    def replace_repeated(match):
        char = match.group(0)[0]
        return char * max_repeat

    # 같은 특수문자가 3회 이상 연속되는 패턴
    pattern = f'({special_chars})\\1{{{max_repeat},}}'
    text = re.sub(pattern, replace_repeated, text)

    return text


# ============================================================================
# 테이블 평탄화 함수 (핵심 기능)
# ============================================================================
def flatten_table_to_sentences(headers, data_rows, table_name=None):
    """
    테이블을 문장 형식으로 평탄화

    예시:
    headers = ['A', 'B', 'C']
    data_rows = [['넓이', '1', '2', '3'], ['길이', '2', '3', '4']]

    결과: "A 넓이 1, B 넓이 2, C 넓이 3, A 길이 2, B 길이 3, C 길이 4"

    Args:
        headers: 컬럼 헤더 리스트
        data_rows: 데이터 행 리스트 (각 행의 첫 번째 값은 행 이름)
        table_name: 테이블명 (선택사항)

    Returns:
        평탄화된 문장 문자열
    """
    sentences = []

    # 테이블명 추가
    if table_name:
        sentences.append(f"[{table_name}]")

    for row in data_rows:
        if not row:
            continue

        # 첫 번째 셀은 행 이름 (row label)
        row_name = row[0] if row else ""

        # 나머지 셀들을 헤더와 매핑
        for col_idx, cell_value in enumerate(row[1:], start=0):
            if col_idx < len(headers):
                col_name = headers[col_idx]
            else:
                col_name = f"열{col_idx+1}"

            # 빈 값이나 "-"는 스킵
            if cell_value and cell_value.strip() and cell_value.strip() != "-":
                # "컬럼명 행명 값" 형식
                sentence = f"{col_name} {row_name} {cell_value}"
                sentences.append(sentence)

    # 쉼표로 연결
    if sentences:
        if table_name:
            # 테이블명이 있으면 첫 번째 요소는 테이블명
            return sentences[0] + " " + ", ".join(sentences[1:])
        else:
            return ", ".join(sentences)

    return ""


def parse_table_to_flattened_text(table, table_name=None):
    """
    테이블을 RAG 청킹에 용이한 평탄화된 텍스트로 변환

    Args:
        table: BeautifulSoup 테이블 요소
        table_name: 테이블명 (선택사항)

    Returns:
        평탄화된 문장 문자열
    """
    # 헤더 추출
    headers = []
    thead = table.find('thead')
    if thead:
        header_rows = thead.find_all('tr')
        if header_rows:
            last_header_row = header_rows[-1]
            for th in last_header_row.find_all(['th', 'td', 'te', 'tu']):
                text = clean_text(th.get_text())
                if text and text != '':
                    headers.append(text)

    # 헤더가 없으면 tbody에서 첫 행을 헤더로 간주
    tbody = table.find('tbody')
    first_row_is_header = False
    if not headers and tbody:
        first_row = tbody.find('tr')
        if first_row:
            for td in first_row.find_all(['td', 'th', 'te', 'tu']):
                text = clean_text(td.get_text())
                if text:
                    headers.append(text)
            first_row_is_header = True

    # 본문 데이터 추출
    data_rows = []
    if tbody:
        rows = tbody.find_all('tr')
        start_idx = 1 if first_row_is_header else 0

        for row in rows[start_idx:]:
            cells = row.find_all(['td', 'th', 'te', 'tu'])
            row_data = []
            for cell in cells:
                text = clean_text(cell.get_text())
                row_data.append(text if text else "-")

            # 빈 행이 아닌 경우만 추가
            if row_data and any(cell != "-" for cell in row_data):
                data_rows.append(row_data)

    # 테이블이 비어있는지 확인
    if not headers and not data_rows:
        return ""

    if headers and not data_rows:
        return ""

    # 단위 정보 추출
    unit_info = ""
    if thead and len(thead.find_all('tr')) > 1:
        first_row = thead.find_all('tr')[0]
        first_row_text = clean_text(first_row.get_text())
        if '단위' in first_row_text:
            unit_info = f"({first_row_text}) "

    # 테이블 평탄화
    flattened = flatten_table_to_sentences(headers, data_rows, table_name)

    if flattened:
        return unit_info + flattened

    return ""


# ============================================================================
# Recursive Chunking 함수
# ============================================================================
def recursive_chunk_text(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP,
                         min_size=MIN_CHUNK_SIZE):
    """
    텍스트를 재귀적으로 청킹

    우선순위:
    1. 문단 단위 (\n\n)
    2. 문장 단위 (. ! ?)
    3. 구 단위 (, ; :)
    4. 단어 단위 (공백)
    5. 문자 단위 (최후의 수단)

    Args:
        text: 청킹할 텍스트
        chunk_size: 목표 청크 크기
        overlap: 청크 간 오버랩 크기
        min_size: 최소 청크 크기

    Returns:
        청크 리스트
    """
    if not text or len(text.strip()) == 0:
        return []

    text = text.strip()

    # 텍스트가 청크 크기보다 작으면 그대로 반환
    if len(text) <= chunk_size:
        return [text]

    chunks = []

    # 분리자 우선순위
    separators = [
        '\n\n',  # 문단
        '\n',    # 줄바꿈
        '。',    # 한글 마침표
        '. ',    # 영어 마침표 + 공백
        '! ',    # 느낌표
        '? ',    # 물음표
        ', ',    # 쉼표
        '; ',    # 세미콜론
        ': ',    # 콜론
        ' ',     # 공백
    ]

    def split_with_separator(text, separator):
        """분리자로 텍스트를 분할하고 분리자를 유지"""
        if separator not in text:
            return [text]

        parts = text.split(separator)
        result = []
        for i, part in enumerate(parts):
            if i < len(parts) - 1:
                result.append(part + separator)
            else:
                if part:  # 마지막 부분이 비어있지 않으면 추가
                    result.append(part)
        return result

    def recursive_split(text, sep_idx=0):
        """재귀적으로 분할"""
        if len(text) <= chunk_size:
            return [text]

        if sep_idx >= len(separators):
            # 모든 분리자를 시도했으면 강제로 자름
            result = []
            for i in range(0, len(text), chunk_size - overlap):
                chunk = text[i:i + chunk_size]
                if chunk:
                    result.append(chunk)
            return result

        separator = separators[sep_idx]
        parts = split_with_separator(text, separator)

        if len(parts) == 1:
            # 이 분리자로 분할되지 않으면 다음 분리자 시도
            return recursive_split(text, sep_idx + 1)

        # 청크 병합
        result = []
        current_chunk = ""

        for part in parts:
            if len(current_chunk) + len(part) <= chunk_size:
                current_chunk += part
            else:
                if current_chunk:
                    # 현재 청크가 너무 크면 재귀 분할
                    if len(current_chunk) > chunk_size:
                        result.extend(recursive_split(current_chunk, sep_idx + 1))
                    else:
                        result.append(current_chunk)

                # 새 청크 시작 (오버랩 적용)
                if overlap > 0 and result:
                    # 이전 청크의 마지막 부분을 오버랩으로 포함
                    prev_chunk = result[-1]
                    overlap_text = prev_chunk[-overlap:] if len(prev_chunk) > overlap else prev_chunk
                    current_chunk = overlap_text + part
                else:
                    current_chunk = part

        # 마지막 청크 처리
        if current_chunk:
            if len(current_chunk) > chunk_size:
                result.extend(recursive_split(current_chunk, sep_idx + 1))
            elif len(current_chunk) >= min_size:
                result.append(current_chunk)
            elif result:
                # 너무 작은 청크는 이전 청크에 병합
                result[-1] = result[-1] + current_chunk
            else:
                result.append(current_chunk)

        return result

    chunks = recursive_split(text)

    # 최소 크기 미만의 청크 처리
    final_chunks = []
    for chunk in chunks:
        chunk = chunk.strip()
        if len(chunk) >= min_size:
            final_chunks.append(chunk)
        elif final_chunks:
            final_chunks[-1] = final_chunks[-1] + " " + chunk
        elif chunk:
            final_chunks.append(chunk)

    return final_chunks


# ============================================================================
# 테이블명 추출 함수
# ============================================================================
def get_table_name_from_prev_p(table_group):
    """
    TABLE-GROUP 앞의 P 태그에서 테이블명 추출
    """
    prev_sibling = table_group.find_previous_sibling()
    while prev_sibling:
        if hasattr(prev_sibling, 'name') and prev_sibling.name and prev_sibling.name.lower() == 'p':
            prev_text = clean_text(prev_sibling.get_text())

            if not prev_text or len(prev_text) < 3:
                prev_sibling = prev_sibling.find_previous_sibling()
                continue

            if prev_text.startswith('※'):
                prev_sibling = prev_sibling.find_previous_sibling()
                continue

            if re.match(r'^\(제\s*\d+\s*기\)$', prev_text):
                prev_sibling = prev_sibling.find_previous_sibling()
                continue

            table_name = prev_text

            item_pattern = re.match(r'^([가-힣]\.|\([0-9]+\)|[0-9]+\)|\-)\s*', table_name)
            if item_pattern:
                prefix = item_pattern.group(0)
                rest = table_name[len(prefix):]
                for sep in [' 당 ', ' 회사의 ', '(단위', '  ']:
                    if sep in rest:
                        rest = rest.split(sep)[0]
                        break
                table_name = prefix + rest
            else:
                for sep in ['. ', '(단위', '  ']:
                    if sep in table_name:
                        table_name = table_name.split(sep)[0]
                        break

            return table_name.strip()[:50]

        prev_sibling = prev_sibling.find_previous_sibling()

    parent = table_group.parent
    while parent:
        if parent.name and parent.name.lower().startswith('section'):
            parent_title = parent.find('title', recursive=False)
            if parent_title:
                return clean_text(parent_title.get_text())[:50]
            break
        parent = parent.parent

    return None


# ============================================================================
# SECTION 파싱 함수
# ============================================================================
def parse_section_content(section_element):
    """
    SECTION 요소의 내용을 파싱 (재귀적으로 하위 SECTION 처리)
    테이블은 평탄화된 문장으로 변환
    """
    output_lines = []

    # TITLE 추출
    title_tag = section_element.find('title', recursive=False)
    if title_tag:
        title = clean_text(title_tag.get_text())
        if title:
            section_name = section_element.name
            if 'section-1' in section_name.lower():
                output_lines.append(f"\n=== {title} ===\n")
            elif 'section-2' in section_name.lower():
                output_lines.append(f"\n--- {title} ---\n")
            elif 'section-3' in section_name.lower():
                output_lines.append(f"\n### {title}\n")
            else:
                output_lines.append(f"\n## {title}\n")

    def process_element(element, current_table_name=None):
        """요소를 재귀적으로 처리"""
        result_lines = []

        for child in element.children:
            if isinstance(child, NavigableString):
                continue

            child_name = child.name.lower() if child.name else ""

            if child_name in ['library', 'table-group']:
                if child_name == 'table-group':
                    group_title = child.find('title', recursive=False)
                    table_name_for_group = None

                    if group_title:
                        title_text = clean_text(group_title.get_text())
                        if title_text:
                            result_lines.append(f"\n[{title_text}]")

                        if group_title.get('atoc') == 'N' or group_title.get('ATOC') == 'N':
                            table_name_for_group = title_text
                    else:
                        table_name_for_group = get_table_name_from_prev_p(child)

                    result_lines.extend(process_element(child, table_name_for_group))
                else:
                    result_lines.extend(process_element(child, current_table_name))

            elif child_name.startswith('section-'):
                subsection_content = parse_section_content(child)
                result_lines.append(subsection_content)

            elif child_name == 'p':
                text = clean_text(child.get_text())
                if text:
                    result_lines.append(text)
                    result_lines.append("")

            elif child_name == 'table':
                # 테이블을 평탄화된 문장으로 변환
                table_text = parse_table_to_flattened_text(child, current_table_name)
                if table_text:
                    result_lines.append(table_text)
                    result_lines.append("")

            elif child_name == 'image':
                img_tag = child.find('img')
                caption_tag = child.find('img-caption')
                if img_tag and img_tag.string:
                    result_lines.append(f"[이미지: {clean_text(img_tag.string)}]")
                if caption_tag:
                    result_lines.append(f"({clean_text(caption_tag.get_text())})")
                result_lines.append("")

        return result_lines

    output_lines.extend(process_element(section_element))
    result = "\n".join(output_lines)
    return compress_repeated_chars(result)


def generate_toc(soup):
    """전체 문서의 목차 생성"""
    toc_lines = ["=== 목차 ===", ""]

    def add_section_to_toc(section, base_indent=0):
        section_name = section.name.lower()
        level = int(section_name.split('-')[1])
        indent = "  " * (level - 1 + base_indent)

        title_tag = section.find('title', recursive=False)
        if title_tag:
            title = clean_text(title_tag.get_text())
            if title:
                toc_lines.append(f"{indent}{title}")

        for child in section.children:
            if not hasattr(child, 'name') or not child.name:
                continue

            child_name = child.name.lower()

            if child_name.startswith('section-'):
                add_section_to_toc(child, base_indent)
            elif child_name == 'library':
                for lib_child in child.children:
                    if hasattr(lib_child, 'name') and lib_child.name:
                        lib_child_name = lib_child.name.lower()
                        if lib_child_name.startswith('section-'):
                            add_section_to_toc(lib_child, base_indent)
                        elif lib_child_name == 'table-group':
                            group_title = lib_child.find('title', recursive=False)
                            if group_title:
                                group_title_text = clean_text(group_title.get_text())
                                if group_title_text:
                                    group_indent = "  " * (level + base_indent)
                                    toc_lines.append(f"{group_indent}{group_title_text}")
            elif child_name == 'table-group':
                group_title = child.find('title', recursive=False)
                if group_title:
                    group_title_text = clean_text(group_title.get_text())
                    if group_title_text:
                        group_indent = "  " * (level + base_indent)
                        toc_lines.append(f"{group_indent}{group_title_text}")

    body = soup.find('body')
    if body:
        for child in body.children:
            if hasattr(child, 'name') and child.name and re.match(r'^section-\d+$', child.name, re.IGNORECASE):
                add_section_to_toc(child)

    toc_lines.append("")
    return "\n".join(toc_lines)


def parse_full_document(xml_content):
    """전체 XML 문서 파싱"""
    soup = BeautifulSoup(xml_content, 'html.parser')

    output_lines = []

    doc_name = soup.find('document-name')
    company_name = soup.find('company-name')

    if doc_name or company_name:
        parts = []
        if company_name:
            parts.append(clean_text(company_name.get_text()))
        if doc_name:
            parts.append(clean_text(doc_name.get_text()))
        output_lines.append(f"=== {' - '.join(parts)} ===")
        output_lines.append("")

    toc = generate_toc(soup)
    output_lines.append(toc)

    body = soup.find('body')
    if body:
        sections = []
        for child in body.children:
            if hasattr(child, 'name') and child.name and re.match(r'^section-\d+$', child.name, re.IGNORECASE):
                sections.append(child)

        for section in sections:
            section_content = parse_section_content(section)
            output_lines.append(section_content)
            output_lines.append("\n")

    result = "\n".join(output_lines)
    return compress_repeated_chars(result)


def split_by_sections(xml_content):
    """각 SECTION-1을 개별 파일로 분리"""
    soup = BeautifulSoup(xml_content, 'html.parser')
    sections_dict = {}

    body = soup.find('body')
    if not body:
        return sections_dict

    sections = body.find_all('section-1', recursive=False)

    for idx, section in enumerate(sections, 1):
        title_tag = section.find('title', recursive=False)
        title = clean_text(title_tag.get_text()) if title_tag else f"Section {idx}"

        safe_title = re.sub(r'[^\w가-힣\s\-]', '', title)[:50]
        section_id = f"section_{idx:02d}_{safe_title}"

        content = parse_section_content(section)

        sections_dict[section_id] = {
            'title': title,
            'content': content,
            'order': idx
        }

    return sections_dict


def create_chunks_from_document(xml_content, chunk_size=CHUNK_SIZE):
    """
    문서 전체를 청크로 분할

    Returns:
        청크 리스트: [{'chunk_id': str, 'section': str, 'content': str}, ...]
    """
    soup = BeautifulSoup(xml_content, 'html.parser')
    chunks = []
    chunk_id = 0

    # 문서 메타 정보
    company_name = soup.find('company-name')
    doc_name = soup.find('document-name')
    meta_prefix = ""
    if company_name:
        meta_prefix += clean_text(company_name.get_text()) + " "
    if doc_name:
        meta_prefix += clean_text(doc_name.get_text()) + " "

    body = soup.find('body')
    if not body:
        return chunks

    sections = body.find_all('section-1', recursive=False)

    for section in sections:
        section_title = ""
        title_tag = section.find('title', recursive=False)
        if title_tag:
            section_title = clean_text(title_tag.get_text())

        # 섹션 내용 파싱
        section_content = parse_section_content(section)

        # 청킹 수행
        section_chunks = recursive_chunk_text(section_content, chunk_size)

        for chunk_content in section_chunks:
            chunk_id += 1
            chunks.append({
                'chunk_id': f"chunk_{chunk_id:04d}",
                'section': section_title,
                'meta': meta_prefix.strip(),
                'content': chunk_content.strip()
            })

    return chunks


def main():
    import sys

    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = "/Users/koscom/Desktop/dev/MyNarrative/data/raw/financial_report/108860_셀바스AI_quarterly_2024Q1_20240513000632.xml"

    input_filename = os.path.basename(input_file)
    dir_name = os.path.splitext(input_filename)[0]

    base_output_dir = "/Users/koscom/Desktop/dev/MyNarrative/devplace/dbs"
    output_dir = os.path.join(base_output_dir, dir_name)

    if len(sys.argv) > 2:
        output_dir = sys.argv[2]

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    print(f"파일 읽는 중: {input_file}")
    print(f"출력 디렉토리: {output_dir}")

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()

        # 1. 전체 문서 파싱 (평탄화된 테이블 포함)
        print("\n전체 문서 파싱 중 (테이블 평탄화 적용)...")
        full_content = parse_full_document(xml_content)

        soup = BeautifulSoup(xml_content, 'html.parser')
        company_name = soup.find('company-name')
        doc_name = soup.find('document-name')

        filename_parts = []
        if company_name:
            name = clean_text(company_name.get_text())
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

        full_output_file = os.path.join(output_dir, output_filename)
        with open(full_output_file, 'w', encoding='utf-8') as f:
            f.write(full_content)
        print(f"전체 문서 저장: {full_output_file}")

        # 2. SECTION별 분리
        print("\nSECTION별 분리 중...")
        sections = split_by_sections(xml_content)

        for section_id, section_data in sections.items():
            section_file = os.path.join(output_dir, f"{section_id}.txt")
            with open(section_file, 'w', encoding='utf-8') as f:
                f.write(section_data['content'])
            print(f"  - {section_data['title']} → {section_id}.txt")

        # 3. 목차 파일 생성
        index_file = os.path.join(output_dir, "index.txt")
        with open(index_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("섹션별 파일 목록\n")
            f.write("="*80 + "\n\n")

            for section_id, section_data in sorted(sections.items(), key=lambda x: x[1]['order']):
                file_path = os.path.join(output_dir, f"{section_id}.txt")
                file_size = os.path.getsize(file_path) / 1024
                f.write(f"{section_data['order']:2d}. {section_data['title']}\n")
                f.write(f"    파일: {section_id}.txt ({file_size:.1f} KB)\n\n")

        print(f"\n목차 파일 저장: {index_file}")

        # 통계 출력
        print(f"\n{'='*60}")
        print("처리 완료!")
        print(f"{'='*60}")
        print(f"총 섹션 수: {len(sections)}개")

        total_size = sum(os.path.getsize(os.path.join(output_dir, f"{sid}.txt"))
                        for sid in sections.keys()) / 1024
        print(f"섹션 파일 총 크기: {total_size:.1f} KB")

        full_size = os.path.getsize(full_output_file) / 1024
        print(f"전체 문서 크기: {full_size:.1f} KB")

        print(f"\n출력 디렉토리: {output_dir}")

    except FileNotFoundError:
        print(f"오류: 파일을 찾을 수 없습니다 - {input_file}")
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
