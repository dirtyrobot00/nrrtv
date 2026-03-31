#!/usr/bin/env python3
"""
RAG용 텍스트 청킹 프로그램

다양한 청킹 전략을 지원하며, LangChain 라이브러리를 활용합니다.

지원 전략:
- recursive: RecursiveCharacterTextSplitter (기본, 가장 범용적)
- character: CharacterTextSplitter (단순 문자 기반)
- token: TokenTextSplitter (토큰 기반)
- sentence: 문장 기반 분할
- semantic: SemanticChunker (임베딩 기반 의미적 분할)

사용 예시:
    python chunker.py <input_path> <output_dir> [--strategy recursive] [--chunk_size 1000] [--chunk_overlap 200]
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class Chunk:
    """청크 데이터 클래스"""
    id: int
    content: str
    metadata: Dict[str, Any]
    char_count: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ChunkerBase:
    """청킹 베이스 클래스"""

    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def split(self, text: str, metadata: Optional[Dict] = None) -> List[Chunk]:
        """텍스트를 청크로 분할"""
        raise NotImplementedError


class RecursiveChunker(ChunkerBase):
    """RecursiveCharacterTextSplitter 기반 청킹"""

    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200):
        super().__init__(chunk_size, chunk_overlap)
        try:
            from langchain_text_splitters import RecursiveCharacterTextSplitter
            self.splitter = RecursiveCharacterTextSplitter(
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                separators=["\n\n", "\n", ".", "!", "?", ",", " ", ""],
                length_function=len,
            )
        except ImportError:
            # fallback: langchain 없이 직접 구현
            self.splitter = None

    def split(self, text: str, metadata: Optional[Dict] = None) -> List[Chunk]:
        if self.splitter:
            docs = self.splitter.create_documents([text])
            chunks = []
            for i, doc in enumerate(docs):
                chunk_metadata = metadata.copy() if metadata else {}
                chunk_metadata['strategy'] = 'recursive'
                chunk_metadata['chunk_index'] = i
                chunks.append(Chunk(
                    id=i,
                    content=doc.page_content,
                    metadata=chunk_metadata,
                    char_count=len(doc.page_content)
                ))
            return chunks
        else:
            return self._fallback_split(text, metadata)

    def _fallback_split(self, text: str, metadata: Optional[Dict] = None) -> List[Chunk]:
        """LangChain 없이 재귀적 분할 구현"""
        separators = ["\n\n", "\n", ".", " ", ""]
        return self._split_recursive(text, separators, metadata)

    def _split_recursive(self, text: str, separators: List[str], metadata: Optional[Dict]) -> List[Chunk]:
        chunks = []
        final_chunks = []

        separator = separators[0] if separators else ""
        new_separators = separators[1:] if len(separators) > 1 else []

        splits = text.split(separator) if separator else list(text)

        current_chunk = ""
        for split in splits:
            piece = split + separator if separator else split

            if len(current_chunk) + len(piece) <= self.chunk_size:
                current_chunk += piece
            else:
                if current_chunk:
                    if len(current_chunk) > self.chunk_size and new_separators:
                        sub_chunks = self._split_recursive(current_chunk, new_separators, metadata)
                        final_chunks.extend(sub_chunks)
                    else:
                        final_chunks.append(current_chunk)
                current_chunk = piece

        if current_chunk:
            if len(current_chunk) > self.chunk_size and new_separators:
                sub_chunks = self._split_recursive(current_chunk, new_separators, metadata)
                final_chunks.extend(sub_chunks)
            else:
                final_chunks.append(current_chunk)

        # 오버랩 적용 및 Chunk 객체 생성
        result = []
        for i, chunk_text in enumerate(final_chunks):
            chunk_metadata = metadata.copy() if metadata else {}
            chunk_metadata['strategy'] = 'recursive'
            chunk_metadata['chunk_index'] = i
            result.append(Chunk(
                id=i,
                content=chunk_text.strip(),
                metadata=chunk_metadata,
                char_count=len(chunk_text.strip())
            ))

        return result


class CharacterChunker(ChunkerBase):
    """CharacterTextSplitter 기반 청킹"""

    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200, separator: str = "\n"):
        super().__init__(chunk_size, chunk_overlap)
        self.separator = separator
        try:
            from langchain_text_splitters import CharacterTextSplitter
            self.splitter = CharacterTextSplitter(
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                separator=separator,
                length_function=len,
            )
        except ImportError:
            self.splitter = None

    def split(self, text: str, metadata: Optional[Dict] = None) -> List[Chunk]:
        if self.splitter:
            docs = self.splitter.create_documents([text])
            chunks = []
            for i, doc in enumerate(docs):
                chunk_metadata = metadata.copy() if metadata else {}
                chunk_metadata['strategy'] = 'character'
                chunk_metadata['chunk_index'] = i
                chunks.append(Chunk(
                    id=i,
                    content=doc.page_content,
                    metadata=chunk_metadata,
                    char_count=len(doc.page_content)
                ))
            return chunks
        else:
            return self._fallback_split(text, metadata)

    def _fallback_split(self, text: str, metadata: Optional[Dict] = None) -> List[Chunk]:
        """LangChain 없이 문자 기반 분할"""
        chunks = []
        start = 0
        text_len = len(text)

        while start < text_len:
            end = min(start + self.chunk_size, text_len)

            # separator 위치 찾기
            if end < text_len:
                sep_pos = text.rfind(self.separator, start, end)
                if sep_pos > start:
                    end = sep_pos + len(self.separator)

            chunk_text = text[start:end].strip()
            if chunk_text:
                chunk_metadata = metadata.copy() if metadata else {}
                chunk_metadata['strategy'] = 'character'
                chunk_metadata['chunk_index'] = len(chunks)
                chunks.append(Chunk(
                    id=len(chunks),
                    content=chunk_text,
                    metadata=chunk_metadata,
                    char_count=len(chunk_text)
                ))

            start = end - self.chunk_overlap if end < text_len else end

        return chunks


class TokenChunker(ChunkerBase):
    """토큰 기반 청킹 (tiktoken 사용)"""

    def __init__(self, chunk_size: int = 500, chunk_overlap: int = 100, encoding_name: str = "cl100k_base"):
        super().__init__(chunk_size, chunk_overlap)
        self.encoding_name = encoding_name
        try:
            from langchain_text_splitters import TokenTextSplitter
            self.splitter = TokenTextSplitter(
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                encoding_name=encoding_name,
            )
        except ImportError:
            self.splitter = None
            try:
                import tiktoken
                self.encoding = tiktoken.get_encoding(encoding_name)
            except ImportError:
                self.encoding = None

    def split(self, text: str, metadata: Optional[Dict] = None) -> List[Chunk]:
        if self.splitter:
            docs = self.splitter.create_documents([text])
            chunks = []
            for i, doc in enumerate(docs):
                # 불완전한 UTF-8 문자 정리 (한글 등 멀티바이트 문자 보호)
                content = self._fix_incomplete_chars(doc.page_content)
                chunk_metadata = metadata.copy() if metadata else {}
                chunk_metadata['strategy'] = 'token'
                chunk_metadata['chunk_index'] = i
                chunks.append(Chunk(
                    id=i,
                    content=content,
                    metadata=chunk_metadata,
                    char_count=len(content)
                ))
            return chunks
        elif self.encoding:
            return self._fallback_split(text, metadata)
        else:
            print("경고: tiktoken이 설치되지 않아 character 방식으로 대체합니다.")
            fallback = CharacterChunker(self.chunk_size * 4, self.chunk_overlap * 4)
            return fallback.split(text, metadata)

    def _fallback_split(self, text: str, metadata: Optional[Dict] = None) -> List[Chunk]:
        """tiktoken으로 직접 토큰 분할"""
        tokens = self.encoding.encode(text)
        chunks = []
        start = 0

        while start < len(tokens):
            end = min(start + self.chunk_size, len(tokens))
            chunk_tokens = tokens[start:end]
            chunk_text = self.encoding.decode(chunk_tokens)

            # 불완전한 UTF-8 문자 정리 (한글 등 멀티바이트 문자 보호)
            chunk_text = self._fix_incomplete_chars(chunk_text)

            chunk_metadata = metadata.copy() if metadata else {}
            chunk_metadata['strategy'] = 'token'
            chunk_metadata['chunk_index'] = len(chunks)
            chunk_metadata['token_count'] = len(chunk_tokens)

            chunks.append(Chunk(
                id=len(chunks),
                content=chunk_text,
                metadata=chunk_metadata,
                char_count=len(chunk_text)
            ))

            start = end - self.chunk_overlap if end < len(tokens) else end

        return chunks

    def _fix_incomplete_chars(self, text: str) -> str:
        """청크 앞뒤의 불완전한 UTF-8 문자(replacement character) 제거"""
        if not text:
            return text

        # 앞쪽 replacement character 제거
        start_idx = 0
        while start_idx < len(text) and text[start_idx] == '\ufffd':
            start_idx += 1

        # 뒤쪽 replacement character 제거
        end_idx = len(text)
        while end_idx > start_idx and text[end_idx - 1] == '\ufffd':
            end_idx -= 1

        return text[start_idx:end_idx]


class SentenceChunker(ChunkerBase):
    """문장 기반 청킹"""

    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200, sentences_per_chunk: int = 5):
        super().__init__(chunk_size, chunk_overlap)
        self.sentences_per_chunk = sentences_per_chunk

    def _split_into_sentences(self, text: str) -> List[str]:
        """텍스트를 문장으로 분할"""
        import re
        # 한국어 및 영어 문장 분리
        sentence_endings = r'(?<=[.!?。])\s+'
        sentences = re.split(sentence_endings, text)
        return [s.strip() for s in sentences if s.strip()]

    def split(self, text: str, metadata: Optional[Dict] = None) -> List[Chunk]:
        sentences = self._split_into_sentences(text)
        chunks = []
        current_sentences = []
        current_length = 0

        for sentence in sentences:
            if current_length + len(sentence) > self.chunk_size and current_sentences:
                chunk_text = ' '.join(current_sentences)
                chunk_metadata = metadata.copy() if metadata else {}
                chunk_metadata['strategy'] = 'sentence'
                chunk_metadata['chunk_index'] = len(chunks)
                chunk_metadata['sentence_count'] = len(current_sentences)

                chunks.append(Chunk(
                    id=len(chunks),
                    content=chunk_text,
                    metadata=chunk_metadata,
                    char_count=len(chunk_text)
                ))

                # 오버랩: 마지막 몇 문장 유지
                overlap_sentences = current_sentences[-2:] if len(current_sentences) > 2 else []
                current_sentences = overlap_sentences
                current_length = sum(len(s) for s in current_sentences)

            current_sentences.append(sentence)
            current_length += len(sentence)

        # 마지막 청크
        if current_sentences:
            chunk_text = ' '.join(current_sentences)
            chunk_metadata = metadata.copy() if metadata else {}
            chunk_metadata['strategy'] = 'sentence'
            chunk_metadata['chunk_index'] = len(chunks)
            chunk_metadata['sentence_count'] = len(current_sentences)

            chunks.append(Chunk(
                id=len(chunks),
                content=chunk_text,
                metadata=chunk_metadata,
                char_count=len(chunk_text)
            ))

        return chunks


class SemanticChunker(ChunkerBase):
    """의미 기반 청킹 (임베딩 사용)"""

    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200,
                 breakpoint_threshold_type: str = "percentile",
                 breakpoint_threshold_amount: float = 95):
        super().__init__(chunk_size, chunk_overlap)
        self.breakpoint_threshold_type = breakpoint_threshold_type
        self.breakpoint_threshold_amount = breakpoint_threshold_amount
        self.splitter = None

        try:
            from langchain_experimental.text_splitter import SemanticChunker
            from langchain_openai import OpenAIEmbeddings

            embeddings = OpenAIEmbeddings()
            self.splitter = SemanticChunker(
                embeddings=embeddings,
                breakpoint_threshold_type=breakpoint_threshold_type,
                breakpoint_threshold_amount=breakpoint_threshold_amount,
            )
        except ImportError as e:
            print(f"경고: SemanticChunker를 사용하려면 langchain_experimental, langchain_openai가 필요합니다.")
            print(f"  pip install langchain-experimental langchain-openai")
        except Exception as e:
            print(f"경고: SemanticChunker 초기화 실패 (OPENAI_API_KEY 필요): {e}")

    def split(self, text: str, metadata: Optional[Dict] = None) -> List[Chunk]:
        if self.splitter:
            try:
                docs = self.splitter.create_documents([text])
                chunks = []
                for i, doc in enumerate(docs):
                    chunk_metadata = metadata.copy() if metadata else {}
                    chunk_metadata['strategy'] = 'semantic'
                    chunk_metadata['chunk_index'] = i
                    chunks.append(Chunk(
                        id=i,
                        content=doc.page_content,
                        metadata=chunk_metadata,
                        char_count=len(doc.page_content)
                    ))
                return chunks
            except Exception as e:
                print(f"SemanticChunker 실행 오류: {e}")
                print("recursive 방식으로 대체합니다.")

        # Fallback to recursive
        fallback = RecursiveChunker(self.chunk_size, self.chunk_overlap)
        return fallback.split(text, metadata)


# 전략 매핑
STRATEGY_MAP = {
    'recursive': RecursiveChunker,
    'character': CharacterChunker,
    'token': TokenChunker,
    'sentence': SentenceChunker,
    'semantic': SemanticChunker,
}


def get_chunker(strategy: str, chunk_size: int, chunk_overlap: int, **kwargs) -> ChunkerBase:
    """전략에 따른 청커 인스턴스 반환"""
    if strategy not in STRATEGY_MAP:
        raise ValueError(f"지원하지 않는 전략: {strategy}. 지원 전략: {list(STRATEGY_MAP.keys())}")

    chunker_class = STRATEGY_MAP[strategy]
    return chunker_class(chunk_size=chunk_size, chunk_overlap=chunk_overlap, **kwargs)


def process_file(file_path: str, chunker: ChunkerBase) -> List[Chunk]:
    """단일 파일 처리"""
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()

    metadata = {
        'source_file': os.path.basename(file_path),
        'source_path': file_path,
    }

    return chunker.split(text, metadata)


def process_directory(dir_path: str, chunker: ChunkerBase, extensions: List[str]) -> Dict[str, List[Chunk]]:
    """디렉토리 내 파일들 처리"""
    results = {}
    dir_path = Path(dir_path)

    for ext in extensions:
        for file_path in dir_path.glob(f"**/*{ext}"):
            if file_path.is_file():
                print(f"  처리 중: {file_path.name}")
                chunks = process_file(str(file_path), chunker)
                results[str(file_path)] = chunks

    return results


def save_chunks(chunks: List[Chunk], output_dir: str, base_name: str, strategy: str):
    """청크 결과 저장"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # JSON 저장 (파일명에 전략명 포함)
    json_file = output_path / f"{base_name}_{strategy}_chunks.json"
    chunks_data = {
        'metadata': {
            'created_at': datetime.now().isoformat(),
            'total_chunks': len(chunks),
            'total_chars': sum(c.char_count for c in chunks),
        },
        'chunks': [c.to_dict() for c in chunks]
    }

    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(chunks_data, f, ensure_ascii=False, indent=2)

    # TXT 저장 (사람이 읽기 쉬운 형식, 파일명에 전략명 포함)
    txt_file = output_path / f"{base_name}_{strategy}_chunks.txt"
    with open(txt_file, 'w', encoding='utf-8') as f:
        f.write(f"{'='*80}\n")
        f.write(f"RAG 청크 결과 (전략: {strategy})\n")
        f.write(f"생성일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"총 청크 수: {len(chunks)}개\n")
        f.write(f"총 문자 수: {sum(c.char_count for c in chunks):,}자\n")
        f.write(f"{'='*80}\n\n")

        for chunk in chunks:
            f.write(f"--- Chunk {chunk.id + 1} ---\n")
            f.write(f"[메타데이터] {chunk.metadata}\n")
            f.write(f"[길이] {chunk.char_count}자\n")
            f.write(f"[내용]\n{chunk.content}\n\n")

    return json_file, txt_file


def main():
    parser = argparse.ArgumentParser(
        description='RAG용 텍스트 청킹 프로그램',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  # 단일 파일 처리 (recursive 전략, 기본값)
  python chunker.py input.txt output_dir/

  # 디렉토리 내 모든 .txt 파일 처리
  python chunker.py input_dir/ output_dir/

  # 전략 및 파라미터 지정
  python chunker.py input.txt output_dir/ --strategy sentence --chunk_size 2000

  # 토큰 기반 청킹
  python chunker.py input.txt output_dir/ --strategy token --chunk_size 500

지원 전략:
  recursive  : 재귀적 문자 분할 (기본, 가장 범용적)
  character  : 단순 문자 기반 분할
  token      : 토큰 기반 분할 (tiktoken 필요)
  sentence   : 문장 기반 분할
  semantic   : 의미 기반 분할 (OpenAI API 필요)
        """
    )

    parser.add_argument('input_path', help='입력 파일 또는 디렉토리 경로')
    parser.add_argument('output_dir', help='출력 디렉토리 경로')
    parser.add_argument('--strategy', '-s', default='recursive',
                        choices=list(STRATEGY_MAP.keys()),
                        help='청킹 전략 (기본: recursive)')
    parser.add_argument('--chunk_size', '-c', type=int, default=1000,
                        help='청크 크기 (기본: 1000)')
    parser.add_argument('--chunk_overlap', '-o', type=int, default=200,
                        help='청크 오버랩 (기본: 200)')
    parser.add_argument('--extension', '-e', default='.txt',
                        help='처리할 파일 확장자 (기본: .txt)')

    args = parser.parse_args()

    input_path = Path(args.input_path)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        print(f"오류: 입력 경로가 존재하지 않습니다 - {input_path}")
        sys.exit(1)

    print(f"{'='*60}")
    print(f"RAG 텍스트 청킹")
    print(f"{'='*60}")
    print(f"입력: {input_path}")
    print(f"출력: {output_dir}")
    print(f"전략: {args.strategy}")
    print(f"청크 크기: {args.chunk_size}")
    print(f"청크 오버랩: {args.chunk_overlap}")
    print(f"{'='*60}\n")

    # 청커 생성
    chunker = get_chunker(
        strategy=args.strategy,
        chunk_size=args.chunk_size,
        chunk_overlap=args.chunk_overlap
    )

    if input_path.is_file():
        # 단일 파일 처리
        print(f"파일 처리 중: {input_path.name}")
        chunks = process_file(str(input_path), chunker)

        base_name = input_path.stem
        json_file, txt_file = save_chunks(chunks, str(output_dir), base_name, args.strategy)

        print(f"\n{'='*60}")
        print(f"처리 완료!")
        print(f"{'='*60}")
        print(f"총 청크 수: {len(chunks)}개")
        print(f"평균 청크 크기: {sum(c.char_count for c in chunks) // len(chunks) if chunks else 0}자")
        print(f"JSON 파일: {json_file}")
        print(f"TXT 파일: {txt_file}")

    else:
        # 디렉토리 처리
        extensions = [args.extension] if args.extension.startswith('.') else [f'.{args.extension}']
        print(f"디렉토리 처리 중: {input_path}")
        print(f"대상 확장자: {extensions}\n")

        results = process_directory(str(input_path), chunker, extensions)

        total_chunks = 0
        for file_path, chunks in results.items():
            base_name = Path(file_path).stem
            json_file, txt_file = save_chunks(chunks, str(output_dir), base_name, args.strategy)
            total_chunks += len(chunks)
            print(f"  저장: {base_name} ({len(chunks)}개 청크)")

        print(f"\n{'='*60}")
        print(f"처리 완료!")
        print(f"{'='*60}")
        print(f"처리 파일 수: {len(results)}개")
        print(f"총 청크 수: {total_chunks}개")
        print(f"출력 디렉토리: {output_dir}")


if __name__ == "__main__":
    main()
