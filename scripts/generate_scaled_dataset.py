
import os
import json
import uuid
import random
import argparse
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from faker import Faker

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "scale_presets.yml"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "input" / "scaled_datasets"
BASE_COMPETITION_IDS = [
    2001, 2002, 2003, 2013, 2014, 2015, 2016, 2017, 2019, 2021,
    2152, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012,
    2018, 2020, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029,
    2030, 2031, 2032, 2033, 2034, 2035, 2036, 2037, 2038, 2039,
    2040, 2041, 2042, 2043, 2044, 2045, 2046, 2047, 2048, 2049,
]

def load_scale_config(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def make_payload_wrapper(endpoint: str, data: Any, run_id: str, batch_id: str) -> dict[str, Any]:
    return {
        "endpoint": endpoint,
        "http_status": 200,
        "request_params": {
            "dag_id": "stg_football_raw_app2",
            "run_id": run_id,
            "batch_id": batch_id,
        },
        "response_json": data,
    }
class ScaledDatasetGenerator:
    def __init__(
        self,
        scale_name: str,
        num_batches: int,
        seed: int = 42,
        output_dir: Optional[Path] = None,
        config_path: Optional[Path] = None,
    ):
        self.scale_name = scale_name
        self.num_batches = num_batches
        self.seed = seed
        self.output_dir = output_dir or DEFAULT_OUTPUT_DIR
        self.config_path = config_path or DEFAULT_CONFIG_PATH
        full_config = load_scale_config(self.config_path)
        scales = full_config.get("scales", {})
        if scale_name not in scales:
            raise ValueError(
                f"Unknown scale '{scale_name}'. Available: {list(scales.keys())}"
            )
        self.config = scales[scale_name]
        self.fake = Faker()
        self.rng = random.Random(seed)
        Faker.seed(seed)
        self.total_files = 0
        self.total_matches = 0
        self.total_teams = 0
    
    def _generate_areas(self) -> list[dict[str, Any]]:
        num_areas = self.config.get("areas", 5)
        areas = []
        for _ in range(num_areas):
            areas.append({
                "id": self.fake.unique.random_int(min=2000, max=3000),
                "name": self.fake.country(),
                "countryCode": self.fake.country_code(),
                "flag": self.fake.image_url(),
            })
        return areas
    
    def _generate_competitions(self, areas: list[dict]) -> list[dict[str, Any]]:
        num_comps = min(self.config.get("competitions", 3), len(BASE_COMPETITION_IDS))
        competition_ids = self.rng.sample(BASE_COMPETITION_IDS, num_comps)
        
        competitions = []
        for c_id in competition_ids:
            competitions.append({
                "id": c_id,
                "area": self.rng.choice(areas),
                "name": self.fake.company() + " League",
                "code": self.fake.lexify(text="???").upper(),
                "type": "LEAGUE",
                "emblem": self.fake.image_url(),
            })
        return competitions
    def _generate_teams(self, competition: dict, num_teams: int) -> list[dict[str, Any]]:
        teams = []
        for _ in range(num_teams):
            teams.append({
                "id": self.fake.unique.random_int(min=10000, max=99999),
                "area": {"id": competition["area"]["id"]},
                "name": self.fake.city() + " FC",
                "shortName": self.fake.city(),
                "tla": self.fake.lexify(text="???").upper(),
                "crest": self.fake.image_url(),
                "address": self.fake.address(),
                "venue": self.fake.company() + " Stadium",
            })
        return teams
    
    def _generate_matches(
        self,
        teams: list[dict],
        competition: dict,
        season_year: int,
        num_matches: int,
    ) -> list[dict[str, Any]]:
        matches = []
        start_date = datetime(season_year, 8, 1)
        
        for m_id in range(num_matches):
            home, away = self.rng.sample(teams, 2)
            matches.append({
                "id": self.fake.unique.random_int(min=100000, max=999999),
                "competition": {"id": competition["id"]},
                "season": {
                    "id": season_year,
                    "startDate": f"{season_year}-08-01",
                    "endDate": f"{season_year + 1}-05-30",
                },
                "utcDate": (start_date + timedelta(days=m_id)).isoformat() + "Z",
                "status": "FINISHED",
                "matchday": (m_id // 10) + 1,
                "stage": "REGULAR_SEASON",
                "homeTeam": {"id": home["id"], "name": home["name"]},
                "awayTeam": {"id": away["id"], "name": away["name"]},
                "score": {
                    "winner": self.rng.choice(["HOME_TEAM", "AWAY_TEAM", "DRAW"]),
                    "fullTime": {
                        "home": self.rng.randint(0, 5),
                        "away": self.rng.randint(0, 5),
                    },
                },
            })
        return matches
    
    def _generate_standings(
        self,
        teams: list[dict],
        competition: dict,
        season_year: int,
    ) -> dict[str, Any]:
        standings_table = []
        for idx, team in enumerate(teams):
            standings_table.append({
                "position": idx + 1,
                "team": {"id": team["id"], "name": team["name"]},
                "playedGames": 38,
                "won": self.rng.randint(0, 20),
                "draw": self.rng.randint(0, 10),
                "lost": self.rng.randint(0, 10),
                "points": self.rng.randint(0, 100),
                "goalsFor": self.rng.randint(20, 80),
                "goalsAgainst": self.rng.randint(20, 80),
                "goalDifference": self.rng.randint(-30, 30),
            })
        
        return {
            "competition": competition,
            "season": {
                "id": season_year,
                "startDate": f"{season_year}-08-01",
                "endDate": f"{season_year + 1}-05-30",
            },
            "standings": [
                {
                    "stage": "REGULAR_SEASON",
                    "type": "TOTAL",
                    "table": standings_table,
                }
            ],
        }
    def _save_batch(
        self,
        batch_id: str,
        batch_num: int,
        areas: list[dict],
        competitions: list[dict],
        comp_data: list[dict], 
    ) -> dict[str, Any]:
        batch_dir = self.output_dir / f"batch_{batch_num:03d}"
        payloads_dir = batch_dir / "payloads"
        payloads_dir.mkdir(parents=True, exist_ok=True)
        
        manifest = {
            "batch_id": batch_id,
            "batch_num": batch_num,
            "scale": self.scale_name,
            "exported_at_utc": datetime.utcnow().isoformat() + "Z",
            "files": [],
        }
        
        file_counter = 1
        batch_files = 0
        
        def add_file(endpoint: str, data: Any, name_hint: str) -> None:
            nonlocal file_counter, batch_files
            filename = f"{file_counter:06d}_{name_hint}.json"
            filepath = payloads_dir / filename
            
            wrapper = make_payload_wrapper(endpoint, data, batch_id, batch_id)
            
            with filepath.open("w", encoding="utf-8") as f:
                json.dump(wrapper, f, ensure_ascii=False, indent=2)
            
            manifest["files"].append({
                "file": f"payloads/{filename}",
                "endpoint": endpoint,
                "http_status": 200,
            })
            file_counter += 1
            batch_files += 1
        if batch_num == 1:
            add_file("areas", {"areas": areas}, "areas")
            add_file("competitions", {"competitions": competitions}, "competitions")
        batch_teams = 0
        batch_matches = 0
        batch_standings = 0
        
        for comp, teams, season_year in comp_data:
            suffix = f"comp_{comp['id']}_season_{season_year}"
            add_file(
                f"competitions/{comp['id']}/teams?season={season_year}",
                {"teams": teams},
                f"teams_{suffix}",
            )
            batch_teams += len(teams)
            num_matches = self.config.get("matches_per_season", 38)
            matches = self._generate_matches(teams, comp, season_year, num_matches)
            add_file(
                f"competitions/{comp['id']}/matches?season={season_year}",
                {"matches": matches},
                f"matches_{suffix}",
            )
            batch_matches += len(matches)
            standings = self._generate_standings(teams, comp, season_year)
            add_file(
                f"competitions/{comp['id']}/standings?season={season_year}",
                standings,
                f"standings_{suffix}",
            )
            batch_standings += 1
        
        manifest["counts"] = {
            "files": batch_files,
            "teams": batch_teams,
            "matches": batch_matches,
            "standings": batch_standings,
        }
        manifest_path = batch_dir / "manifest.json"
        with manifest_path.open("w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        
        return {
            "batch_id": batch_id,
            "batch_num": batch_num,
            "files": batch_files,
            "teams": batch_teams,
            "matches": batch_matches,
        }
    def generate(self) -> dict[str, Any]:
        print(f"\n{'='*60}")
        print(f"Генерация датасета: scale={self.scale_name}, batches={self.num_batches}")
        print(f"Конфигурация: {self.config.get('description', 'N/A')}")
        print(f"{'='*60}\n")
        
        start_time = datetime.now()
        dataset_id = f"scaled_{self.scale_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.output_dir = self.output_dir / dataset_id
        self.output_dir.mkdir(parents=True, exist_ok=True)
        print("Генерация областей (areas)...")
        areas = self._generate_areas()
        print(f"  Сгенерировано областей: {len(areas)}")
        print("Генерация соревнований (competitions)...")
        competitions = self._generate_competitions(areas)
        print(f"  Сгенерировано соревнований: {len(competitions)}")
        seasons = self.config.get("seasons", [2023, 2024])
        teams_per_comp = self.config.get("teams_per_comp", 8)
        all_tasks = []
        for comp in competitions:
            teams = self._generate_teams(comp, teams_per_comp)
            self.total_teams += len(teams)
            for season_year in seasons:
                all_tasks.append((comp, teams, season_year))
        tasks_per_batch = max(1, len(all_tasks) // self.num_batches)
        batches_tasks = []
        for i in range(self.num_batches):
            start = i * tasks_per_batch
            end = start + tasks_per_batch if i < self.num_batches - 1 else len(all_tasks)
            batches_tasks.append(all_tasks[start:end])
        batch_results = []
        for i, batch_tasks in enumerate(batches_tasks):
            batch_num = i + 1
            batch_id = f"{dataset_id}_b{batch_num:03d}"
            
            print(f"\nГенерация батча {batch_num}/{self.num_batches} ({len(batch_tasks)} задач)...")
            result = self._save_batch(batch_id, batch_num, areas, competitions, batch_tasks)
            batch_results.append(result)
            
            print(f"  Файлов: {result['files']}")
            print(f"  Команд: {result['teams']}")
            print(f"  Матчей: {result['matches']}")
            
            self.total_files += result["files"]
            self.total_matches += result["matches"]
        total_duration = (datetime.now() - start_time).total_seconds()
        
        dataset_manifest = {
            "dataset_id": dataset_id,
            "scale": self.scale_name,
            "description": self.config.get("description", ""),
            "created_at": datetime.utcnow().isoformat() + "Z",
            "generation_duration_sec": round(total_duration, 2),
            "configuration": {
                "competitions": len(competitions),
                "teams_per_comp": teams_per_comp,
                "matches_per_season": self.config.get("matches_per_season", 38),
                "seasons": seasons,
                "num_batches": self.num_batches,
                "seed": self.seed,
            },
            "totals": {
                "files": self.total_files,
                "teams": self.total_teams,
                "matches": self.total_matches,
                "standings": len(all_tasks),
            },
            "batches": batch_results,
        }
        
        manifest_path = self.output_dir / "dataset_manifest.json"
        with manifest_path.open("w", encoding="utf-8") as f:
            json.dump(dataset_manifest, f, indent=2, ensure_ascii=False)
        print(f"\n{'='*60}")
        print(f"Датасет сгенерирован: {dataset_id}")
        print(f"Директория: {self.output_dir}")
        print(f"Время генерации: {total_duration:.1f} сек")
        print(f"Батчей: {self.num_batches}")
        print(f"Всего файлов: {self.total_files}")
        print(f"Всего команд: {self.total_teams}")
        print(f"Всего матчей: {self.total_matches}")
        print(f"{'='*60}\n")
        
        return dataset_manifest

def main():
    parser = argparse.ArgumentParser(
        description="Генератор масштабируемых датасетов для ETL-экспериментов",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  %(prog)s --scale small
  %(prog)s --scale medium --batches 4
  %(prog)s --scale large --batches 10 --seed 42
  %(prog)s --scale small --batches 1 --output-dir /tmp/test_data
  %(prog)s --config my_scale_presets.yml --scale custom
        """,
    )
    parser.add_argument(
        "--scale",
        required=True,
        choices=["small", "medium", "large"],
        help="Масштаб датасета",
    )
    parser.add_argument(
        "--batches",
        type=int,
        default=None,
        help="Количество батчей (по умолчанию из конфига)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Сид для воспроизводимости (default: 42)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Путь к конфигурационному YAML-файлу",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Директория для сохранения датасета",
    )
    
    args = parser.parse_args()
    config_path = args.config or DEFAULT_CONFIG_PATH
    config = load_scale_config(config_path)
    if args.batches is not None:
        num_batches = args.batches
    else:
        batch_config = config.get("batch_config", {})
        num_batches = batch_config.get(args.scale, 1)
    generator = ScaledDatasetGenerator(
        scale_name=args.scale,
        num_batches=num_batches,
        seed=args.seed,
        output_dir=args.output_dir,
        config_path=config_path,
    )
    
    try:
        result = generator.generate()
        print("Генерация завершена успешно!")
        return 0
    except Exception as e:
        print(f"Ошибка генерации: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())