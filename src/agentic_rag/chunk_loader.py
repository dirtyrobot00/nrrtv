#!/usr/bin/env python3
"""
ChromaDB Chunk Loader
=====================
청킹된 JSON 파일을 ChromaDB에 적재하는 프로그램

전략별로 다른 컬렉션에 저장하여 RAG 성능 비교 테스트 가능
"""

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

try:
    import chromadb
    from chromadb.config import Settings
except ImportError:
    print("chromadb가 설치되지 않았습니다. pip install chromadb 실행 필요")
    exit(1)


class ChunkLoader:
    """ChromaDB에 청크를 적재하는 클래스"""

    def __init__(
        self,
        persist_directory: str = "./chroma_db",
        embedding_model: str = "default"
    ):
        """
        Args:
            persist_directory: ChromaDB 저장 경로
            embedding_model: 임베딩 모델 선택
                - "default": ChromaDB 기본 임베딩 (all-MiniLM-L6-v2)
                - "openai": OpenAI text-embedding-ada-002
                - "korean": KoSimCSE 또는 ko-sbert (한국어 특화)
                - "qwen": Qwen3-Embedding-0.6B (다국어, 고성능)
        """
        self.persist_directory = persist_directory
        self.embedding_model = embedding_model

        # ChromaDB 클라이언트 초기화
        self.client = chromadb.PersistentClient(
            path=persist_directory,
            settings=Settings(anonymized_telemetry=False)
        )

        # 임베딩 함수 설정
        self.embedding_function = self._get_embedding_function()

    def _get_embedding_function(self):
        """임베딩 함수 반환"""
        if self.embedding_model == "openai":
            try:
                from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction
                api_key = os.environ.get("OPENAI_API_KEY")
                if not api_key:
                    print("경고: OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")
                    print("기본 임베딩 함수를 사용합니다.")
                    return None
                return OpenAIEmbeddingFunction(
                    api_key=api_key,
                    model_name="text-embedding-ada-002"
                )
            except Exception as e:
                print(f"OpenAI 임베딩 초기화 실패: {e}")
                return None

        elif self.embedding_model == "korean":
            try:
                from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
                # 한국어 특화 모델
                return SentenceTransformerEmbeddingFunction(
                    model_name="jhgan/ko-sbert-nli"
                )
            except Exception as e:
                print(f"한국어 임베딩 초기화 실패: {e}")
                print("sentence-transformers 설치 필요: pip install sentence-transformers")
                return None

        elif self.embedding_model == "qwen":
            try:
                from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
                # Qwen3-Embedding-0.6B (다국어, 고성능)
                print("Qwen3-Embedding-0.6B 모델 로딩 중...")
                return SentenceTransformerEmbeddingFunction(
                    model_name="Qwen/Qwen3-Embedding-0.6B",
                    trust_remote_code=True
                )
            except Exception as e:
                print(f"Qwen 임베딩 초기화 실패: {e}")
                print("sentence-transformers 설치 필요: pip install sentence-transformers")
                return None

        # default: ChromaDB 기본 임베딩 사용
        return None

    def load_chunks_file(self, json_path: str) -> Dict[str, Any]:
        """JSON 청크 파일 로드"""
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data

    def create_collection_name(
        self,
        stock_code: Optional[str] = None,
        stock_name: Optional[str] = None,
        strategy: Optional[str] = None,
        custom_name: Optional[str] = None
    ) -> str:
        """컬렉션 이름 생성

        형식: {stock_code}_{stock_name}_{strategy}
        예: 004560_hyundai_bng_steel_token
        """
        if custom_name:
            # 특수문자 제거 및 소문자 변환
            return self._sanitize_name(custom_name)

        parts = []
        if stock_code:
            parts.append(stock_code)
        if stock_name:
            parts.append(self._sanitize_name(stock_name))
        if strategy:
            parts.append(strategy)

        if not parts:
            parts.append(f"collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

        return "_".join(parts)

    def _sanitize_name(self, name: str) -> str:
        """컬렉션 이름에 사용할 수 없는 문자 제거"""
        # 영문, 숫자, 언더스코어만 허용
        import re
        # 한글은 음역하지 않고 제거
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # 연속된 언더스코어 제거
        sanitized = re.sub(r'_+', '_', sanitized)
        # 앞뒤 언더스코어 제거
        sanitized = sanitized.strip('_')
        # 소문자 변환
        return sanitized.lower() if sanitized else "unnamed"

    def get_or_create_collection(self, collection_name: str):
        """컬렉션 가져오기 또는 생성"""
        if self.embedding_function:
            return self.client.get_or_create_collection(
                name=collection_name,
                embedding_function=self.embedding_function,
                metadata={"hnsw:space": "cosine"}
            )
        else:
            return self.client.get_or_create_collection(
                name=collection_name,
                metadata={"hnsw:space": "cosine"}
            )

    def load_to_collection(
        self,
        json_path: str,
        collection_name: str,
        batch_size: int = 100,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """청크를 컬렉션에 적재

        Args:
            json_path: 청크 JSON 파일 경로
            collection_name: 컬렉션 이름
            batch_size: 배치 크기
            overwrite: 기존 컬렉션 덮어쓰기 여부

        Returns:
            적재 결과 정보
        """
        # JSON 파일 로드
        data = self.load_chunks_file(json_path)
        chunks = data.get('chunks', [])
        file_metadata = data.get('metadata', {})

        if not chunks:
            return {"status": "error", "message": "청크가 없습니다."}

        # 기존 컬렉션 처리
        if overwrite:
            try:
                self.client.delete_collection(collection_name)
                print(f"기존 컬렉션 삭제: {collection_name}")
            except:
                pass

        # 컬렉션 생성/가져오기
        collection = self.get_or_create_collection(collection_name)

        # 기존 데이터 수 확인
        existing_count = collection.count()
        if existing_count > 0 and not overwrite:
            print(f"기존 데이터 {existing_count}개 존재. --overwrite 옵션으로 덮어쓰기 가능")

        # 청크 적재
        total_loaded = 0
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i + batch_size]

            ids = []
            documents = []
            metadatas = []

            for chunk in batch:
                # ID 생성: source_file + chunk_index
                chunk_meta = chunk.get('metadata', {})
                source_file = chunk_meta.get('source_file', 'unknown')
                chunk_index = chunk_meta.get('chunk_index', chunk.get('id', 0))
                chunk_id = f"{source_file}_{chunk_index}"

                ids.append(chunk_id)
                documents.append(chunk.get('content', ''))

                # 메타데이터 구성
                metadata = {
                    "source_file": chunk_meta.get('source_file', ''),
                    "source_path": chunk_meta.get('source_path', ''),
                    "strategy": chunk_meta.get('strategy', ''),
                    "chunk_index": chunk_index,
                    "char_count": chunk.get('char_count', 0),
                    "loaded_at": datetime.now().isoformat()
                }
                metadatas.append(metadata)

            # ChromaDB에 추가
            try:
                collection.add(
                    ids=ids,
                    documents=documents,
                    metadatas=metadatas
                )
                total_loaded += len(batch)
                print(f"  적재 진행: {total_loaded}/{len(chunks)}")
            except Exception as e:
                print(f"  배치 적재 오류: {e}")

        result = {
            "status": "success",
            "collection_name": collection_name,
            "total_chunks": len(chunks),
            "loaded_chunks": total_loaded,
            "file_metadata": file_metadata,
            "persist_directory": self.persist_directory
        }

        return result

    def list_collections(self) -> List[str]:
        """모든 컬렉션 목록 반환"""
        collections = self.client.list_collections()
        return [c.name for c in collections]

    def get_collection_info(self, collection_name: str) -> Dict[str, Any]:
        """컬렉션 정보 조회"""
        try:
            collection = self.client.get_collection(collection_name)
            count = collection.count()

            # 샘플 데이터 조회
            sample = collection.peek(limit=3)

            return {
                "name": collection_name,
                "count": count,
                "sample_ids": sample.get('ids', [])[:3],
                "sample_metadatas": sample.get('metadatas', [])[:3]
            }
        except Exception as e:
            return {"error": str(e)}

    def delete_collection(self, collection_name: str) -> bool:
        """컬렉션 삭제"""
        try:
            self.client.delete_collection(collection_name)
            return True
        except Exception as e:
            print(f"컬렉션 삭제 실패: {e}")
            return False

    def query(
        self,
        collection_name: str,
        query_text: str,
        n_results: int = 5
    ) -> Dict[str, Any]:
        """컬렉션에서 유사 문서 검색

        Args:
            collection_name: 컬렉션 이름
            query_text: 검색 쿼리
            n_results: 반환할 결과 수

        Returns:
            검색 결과
        """
        try:
            if self.embedding_function:
                collection = self.client.get_collection(
                    collection_name,
                    embedding_function=self.embedding_function
                )
            else:
                collection = self.client.get_collection(collection_name)

            results = collection.query(
                query_texts=[query_text],
                n_results=n_results
            )

            return {
                "status": "success",
                "query": query_text,
                "results": [
                    {
                        "id": results['ids'][0][i],
                        "document": results['documents'][0][i][:500] + "..." if len(results['documents'][0][i]) > 500 else results['documents'][0][i],
                        "metadata": results['metadatas'][0][i],
                        "distance": results['distances'][0][i] if 'distances' in results else None
                    }
                    for i in range(len(results['ids'][0]))
                ]
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}


def main():
    parser = argparse.ArgumentParser(
        description="ChromaDB Chunk Loader - 청크 JSON을 ChromaDB에 적재",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  # 기본 적재
  python chunk_loader.py load chunks.json --collection my_collection

  # 종목 정보로 컬렉션 이름 자동 생성
  python chunk_loader.py load chunks.json --stock-code 004560 --stock-name 현대비앤지스틸

  # OpenAI 임베딩 사용
  python chunk_loader.py load chunks.json --collection test --embedding openai

  # 컬렉션 목록 조회
  python chunk_loader.py list

  # 컬렉션 정보 조회
  python chunk_loader.py info my_collection

  # 검색 테스트
  python chunk_loader.py query my_collection "매출액 현황"

  # 컬렉션 삭제
  python chunk_loader.py delete my_collection
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="명령어")

    # load 명령어
    load_parser = subparsers.add_parser("load", help="청크를 ChromaDB에 적재")
    load_parser.add_argument("input", help="입력 JSON 파일 또는 디렉토리")
    load_parser.add_argument("--collection", "-c", help="컬렉션 이름 (직접 지정)")
    load_parser.add_argument("--stock-code", help="종목코드 (컬렉션 이름 자동 생성용)")
    load_parser.add_argument("--stock-name", help="종목명 (컬렉션 이름 자동 생성용)")
    load_parser.add_argument("--db-path", default="./chroma_db", help="ChromaDB 저장 경로")
    load_parser.add_argument("--embedding", "-e", choices=["default", "openai", "korean", "qwen"],
                           default="default", help="임베딩 모델 (qwen: Qwen3-Embedding-0.6B)")
    load_parser.add_argument("--batch-size", "-b", type=int, default=100, help="배치 크기")
    load_parser.add_argument("--overwrite", action="store_true", help="기존 컬렉션 덮어쓰기")

    # list 명령어
    list_parser = subparsers.add_parser("list", help="컬렉션 목록 조회")
    list_parser.add_argument("--db-path", default="./chroma_db", help="ChromaDB 저장 경로")

    # info 명령어
    info_parser = subparsers.add_parser("info", help="컬렉션 정보 조회")
    info_parser.add_argument("collection", help="컬렉션 이름")
    info_parser.add_argument("--db-path", default="./chroma_db", help="ChromaDB 저장 경로")

    # query 명령어
    query_parser = subparsers.add_parser("query", help="유사 문서 검색")
    query_parser.add_argument("collection", help="컬렉션 이름")
    query_parser.add_argument("query_text", help="검색 쿼리")
    query_parser.add_argument("--n-results", "-n", type=int, default=5, help="반환 결과 수")
    query_parser.add_argument("--db-path", default="./chroma_db", help="ChromaDB 저장 경로")
    query_parser.add_argument("--embedding", "-e", choices=["default", "openai", "korean", "qwen"],
                            default="default", help="임베딩 모델 (qwen: Qwen3-Embedding-0.6B)")

    # delete 명령어
    delete_parser = subparsers.add_parser("delete", help="컬렉션 삭제")
    delete_parser.add_argument("collection", help="컬렉션 이름")
    delete_parser.add_argument("--db-path", default="./chroma_db", help="ChromaDB 저장 경로")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # 명령어 실행
    if args.command == "load":
        loader = ChunkLoader(
            persist_directory=args.db_path,
            embedding_model=args.embedding
        )

        input_path = Path(args.input)

        if input_path.is_file():
            # 단일 파일 처리
            # JSON에서 전략 정보 추출
            with open(input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            strategy = None
            if data.get('chunks'):
                chunk_meta = data['chunks'][0].get('metadata', {})
                strategy = chunk_meta.get('strategy', '')

            # 컬렉션 이름 결정
            if args.collection:
                collection_name = args.collection
            else:
                collection_name = loader.create_collection_name(
                    stock_code=args.stock_code,
                    stock_name=args.stock_name,
                    strategy=strategy
                )

            print(f"\n{'='*60}")
            print(f"ChromaDB Chunk Loader")
            print(f"{'='*60}")
            print(f"입력 파일: {input_path}")
            print(f"컬렉션: {collection_name}")
            print(f"DB 경로: {args.db_path}")
            print(f"임베딩: {args.embedding}")
            print(f"{'='*60}\n")

            result = loader.load_to_collection(
                str(input_path),
                collection_name,
                batch_size=args.batch_size,
                overwrite=args.overwrite
            )

            print(f"\n{'='*60}")
            print(f"적재 완료!")
            print(f"{'='*60}")
            print(f"컬렉션: {result['collection_name']}")
            print(f"적재 청크: {result['loaded_chunks']}개")
            print(f"DB 경로: {result['persist_directory']}")

        elif input_path.is_dir():
            # 디렉토리 내 모든 JSON 파일 처리
            json_files = list(input_path.glob("*_chunks.json"))

            print(f"\n{'='*60}")
            print(f"ChromaDB Chunk Loader - 일괄 처리")
            print(f"{'='*60}")
            print(f"입력 디렉토리: {input_path}")
            print(f"발견된 파일: {len(json_files)}개")
            print(f"DB 경로: {args.db_path}")
            print(f"{'='*60}\n")

            for json_file in json_files:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                strategy = None
                if data.get('chunks'):
                    chunk_meta = data['chunks'][0].get('metadata', {})
                    strategy = chunk_meta.get('strategy', '')

                collection_name = loader.create_collection_name(
                    stock_code=args.stock_code,
                    stock_name=args.stock_name,
                    strategy=strategy
                )

                print(f"\n처리 중: {json_file.name}")
                print(f"  컬렉션: {collection_name}")

                result = loader.load_to_collection(
                    str(json_file),
                    collection_name,
                    batch_size=args.batch_size,
                    overwrite=args.overwrite
                )

                print(f"  적재: {result['loaded_chunks']}개 청크")

            print(f"\n{'='*60}")
            print(f"일괄 처리 완료!")
            print(f"{'='*60}")
        else:
            print(f"오류: 파일 또는 디렉토리를 찾을 수 없습니다 - {input_path}")

    elif args.command == "list":
        loader = ChunkLoader(persist_directory=args.db_path)
        collections = loader.list_collections()

        print(f"\n{'='*60}")
        print(f"컬렉션 목록 ({args.db_path})")
        print(f"{'='*60}")

        if collections:
            for name in collections:
                info = loader.get_collection_info(name)
                count = info.get('count', '?')
                print(f"  - {name} ({count}개 문서)")
        else:
            print("  (컬렉션 없음)")

        print()

    elif args.command == "info":
        loader = ChunkLoader(persist_directory=args.db_path)
        info = loader.get_collection_info(args.collection)

        print(f"\n{'='*60}")
        print(f"컬렉션 정보: {args.collection}")
        print(f"{'='*60}")

        if "error" in info:
            print(f"오류: {info['error']}")
        else:
            print(f"문서 수: {info['count']}개")
            print(f"\n샘플 문서 ID:")
            for doc_id in info.get('sample_ids', []):
                print(f"  - {doc_id}")

            print(f"\n샘플 메타데이터:")
            for meta in info.get('sample_metadatas', []):
                print(f"  - {meta}")

        print()

    elif args.command == "query":
        loader = ChunkLoader(
            persist_directory=args.db_path,
            embedding_model=args.embedding
        )

        result = loader.query(
            args.collection,
            args.query_text,
            n_results=args.n_results
        )

        print(f"\n{'='*60}")
        print(f"검색 결과: '{args.query_text}'")
        print(f"컬렉션: {args.collection}")
        print(f"{'='*60}\n")

        if result["status"] == "error":
            print(f"오류: {result['message']}")
        else:
            for i, item in enumerate(result["results"], 1):
                print(f"--- 결과 {i} ---")
                print(f"ID: {item['id']}")
                if item['distance'] is not None:
                    print(f"거리: {item['distance']:.4f}")
                print(f"메타데이터: {item['metadata']}")
                print(f"내용:\n{item['document']}\n")

    elif args.command == "delete":
        loader = ChunkLoader(persist_directory=args.db_path)

        # 확인
        confirm = input(f"컬렉션 '{args.collection}'을 삭제하시겠습니까? (y/N): ")
        if confirm.lower() == 'y':
            if loader.delete_collection(args.collection):
                print(f"컬렉션 '{args.collection}' 삭제 완료")
            else:
                print("삭제 실패")
        else:
            print("취소됨")


if __name__ == "__main__":
    main()
