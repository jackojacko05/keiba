import functions_framework
from google.cloud import storage
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timezone, timedelta
import os
import re
import time
import tempfile

class NetkeibaRaceScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Cookie': 'uid=0; nkauth=0'
        }
        # GCSクライアントの初期化
        self.storage_client = storage.Client()

    def scrape_race_result(self, race_id):
        """
        レース結果をスクレイピングする
        Args:
            race_id (str): レースID (例: "202408060411")
        Returns:
            dict: レース情報と結果のデータ
        """
        url = f'https://db.netkeiba.com/race/{race_id}'
        
        try:
            response = requests.get(url, headers=self.headers)
            response.encoding = 'EUC-JP'
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            race_data = {
                'race_info': self._get_race_info(soup),
                'race_details': self._get_race_details(soup),
                'race_results': self._get_race_results(soup)
            }
            
            return race_data
            
        except requests.exceptions.RequestException as e:
            print(f"Network error while scraping race {race_id}: {str(e)}")
            return None
        except Exception as e:
            print(f"Error scraping race {race_id}: {str(e)}")
            return None

    def _get_race_info(self, soup):
        """レースの基本情報を取得"""
        race_info = {}
        
        # レース名の取得
        race_name_elem = soup.select_one('dl.racedata h1')
        if race_name_elem:
            # imgタグを削除
            for img in race_name_elem.find_all('img'):
                img.decompose()
            race_info['race_name'] = race_name_elem.text.strip()
        
        # 基本情報の取得（日付、開催場所、ラウンド）
        race_details = soup.select_one('div.data_intro p.smalltxt')
        if race_details:
            details_text = race_details.text.strip()
            parts = details_text.split()
            if len(parts) >= 2:
                # 日付の処理を修正（YYYY-MM-DD形式に統一）
                raw_date = parts[0]
                race_info['race_date'] = (pd.to_datetime(raw_date
                                        .replace('年', '-')
                                        .replace('月', '-')
                                        .replace('日', ''))
                                        .strftime('%Y-%m-%d'))
                
                # kaisai_infoを分割（例：6回京都4日目 → 6, 京都, 4）
                kaisai_info = parts[1]
                match = re.match(r'(\d+)回(.+)(\d+)日目', kaisai_info)
                if match:
                    race_info['kaisai_kai'] = int(match.group(1))  # 6回 → 6
                    race_info['kaisai_place'] = match.group(2)     # 京都
                    race_info['kaisai_nichime'] = int(match.group(3))  # 4日目 → 4
                
                race_info['race_conditions'] = ' '.join(parts[2:])
        
        # レース詳細情報の取得
        data_intro = soup.select_one('div.data_intro')
        if data_intro:
            data_lines = [line.strip() for line in data_intro.text.split('\n') if line.strip()]
            for line in data_lines:
                # ダ右1000m / 天候 : 晴 / ダート : 良 / 発走 : 10:20 のような形式に対応
                if ('芝' in line or 'ダ' in line) and 'm' in line and '天候' in line:
                    info_parts = line.split('/')
                    
                    # コース情報を分割
                    if len(info_parts) > 0:
                        course_info = info_parts[0].strip()
                        # 例: "ダ右1000m" → ["ダ右", "1000m"]
                        if 'm' in course_info:
                            track_part = ''
                            direction_part = ''
                            distance_part = ''
                            
                            # 馬場の種類（芝/ダ）を抽出
                            if '芝' in course_info:
                                race_info['track_type'] = '芝'
                                track_part = '芝'
                            elif 'ダ' in course_info:
                                race_info['track_type'] = 'ダ'
                                track_part = 'ダ'
                            
                            # 回り（右/左/直線）を抽出
                            if '右' in course_info:
                                race_info['track_direction'] = '右'
                                direction_part = '右'
                            elif '左' in course_info:
                                race_info['track_direction'] = '左'
                                direction_part = '左'
                            else:
                                race_info['track_direction'] = '直線'
                            
                            # 距離を抽出
                            distance_str = course_info.replace(track_part, '').replace(direction_part, '').replace('m', '').strip()
                            if distance_str.isdigit():
                                race_info['track_distance'] = distance_str
                    
                    # 天候、馬場状態、発走時刻を抽出
                    for part in info_parts:
                        part = part.strip()
                        if '天候' in part:
                            race_info['weather'] = part.split(':')[1].strip()
                        elif ('芝' in part or 'ダート' in part) and ':' in part:
                            race_info['track_condition'] = part.split(':')[1].strip()
                        elif '発走' in part:
                            time_parts = part.split(':')
                            if len(time_parts) >= 2:
                                # HH:MM:SS形式に統一
                                time_str = ':'.join(time_parts[1:]).strip()
                                if len(time_str.split(':')) == 1:
                                    time_str = f"{time_str}:00"
                                race_info['start_time'] = time_str
                    break
        
        # タイムスタンプの生成
        if 'race_date' in race_info and 'start_time' in race_info:
            try:
                # 時刻を:00を付けて秒まで統一
                time_parts = race_info['start_time'].split(':')
                if len(time_parts) == 1:
                    race_info['start_time'] = f"{time_parts[0]}:00:00"
                elif len(time_parts) == 2:
                    race_info['start_time'] = f"{race_info['start_time']}:00"
                
                datetime_str = f"{race_info['race_date']} {race_info['start_time']}"
                try:
                    race_info['timestamp'] = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    # 時刻フォーマットが異なる場合は、時刻部分のみを抽出して処理
                    time_match = re.search(r'(\d{1,2}):(\d{2})', race_info['start_time'])
                    if time_match:
                        hour, minute = time_match.groups()
                        race_info['start_time'] = f"{hour.zfill(2)}:{minute}:00"
                        datetime_str = f"{race_info['race_date']} {race_info['start_time']}"
                        race_info['timestamp'] = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                    else:
                        print(f"Error creating timestamp: Invalid time format - {race_info['start_time']}")
                        race_info['timestamp'] = None
                
            except Exception as e:
                print(f"Error creating timestamp: {e}")
                race_info['timestamp'] = None
        
        # start_timeの処理を修正（HH:MM:SS形式に統一）
        if 'start_time' in race_info and race_info['start_time']:
            race_info['start_time'] = f"{race_info['start_time']}:00"
        
        return race_info

    def _get_race_details(self, soup):
        """レース詳細情報を取得"""
        data_intro = soup.select_one('div.data_intro')
        if not data_intro:
            return {}
            
        details = {
            'course_info': data_intro.select_one('span.race_course').text.strip() if data_intro.select_one('span.race_course') else None,
            'weather': data_intro.select_one('span.weather').text.strip() if data_intro.select_one('span.weather') else None,
            'course_condition': data_intro.select_one('span.course_condition').text.strip() if data_intro.select_one('span.course_condition') else None,
            'race_time': data_intro.select_one('span.race_time').text.strip() if data_intro.select_one('span.race_time') else None
        }
        
        return details

    def _convert_time_to_seconds(self, time_str):
        """
        レースタイムを秒数に変換
        例: "1:34.5" → 94.5
        """
        try:
            if not time_str or time_str == '':
                return 0.0
            
            # コロンで分と秒を分割
            parts = time_str.split(':')
            if len(parts) == 2:
                minutes = float(parts[0])
                seconds = float(parts[1])
                return minutes * 60 + seconds
            else:
                # コロンがない場合は秒だけとして処理
                return float(time_str)
        except ValueError:
            return 0.0

    def _get_race_results(self, soup):
        """レース結果を取得"""
        result_table = soup.select_one('.race_table_01')
        if not result_table:
            result_table = soup.select_one('.RaceTable01')
        
        if not result_table:
            return []
        
        headers = [th.text.strip() for th in result_table.select('tr th')]
        if not headers:
            return []
        
        results = []
        for row in result_table.select('tr'):
            if not row.select('td'):
                continue
            result = {}
            for i, cell in enumerate(row.select('td')):
                if i < len(headers):
                    value = cell.text.strip()
                    header = headers[i]
                    
                    # 着順の処理（数値以外は-1に変換）
                    if header == '着順':
                        try:
                            result[header] = int(value)
                        except ValueError:
                            result[header] = -1
                    
                    # 単勝オッズの処理
                    elif header == '単勝':
                        value = value.replace('---', '-1')  # 特殊な値を-1に置換
                        try:
                            result[header] = float(value)
                        except (ValueError, TypeError):
                            result[header] = -1.0
                    
                    # タイムの処理
                    elif header == 'タイム':
                        result[header] = self._convert_time_to_seconds(value)
                    
                    # 通過順位の処理
                    elif header == '通過':
                        positions = value.split('-')
                        for j in range(4):
                            column_name = f'通過_{j+1}F'
                            result[column_name] = positions[j] if j < len(positions) else ''
                    
                    # 性齢の処理
                    elif header == '性齢':
                        if value:
                            result['性'] = value[0]
                            result['齢'] = value[1:] if len(value) > 1 else ''
                    
                    # 馬体重の処理
                    elif header == '馬体重':
                        if value and '(' in value and ')' in value:
                            weight_parts = value.replace(')', '').split('(')
                            result['馬体重'] = int(weight_parts[0]) if weight_parts[0].isdigit() else 0
                            result['増減'] = int(weight_parts[1]) if weight_parts[1].replace('+', '').replace('-', '').isdigit() else 0
                    
                    # 調教師の処理
                    elif header == '調教師':
                        if value:
                            value = value.replace('\n', '')
                            match = re.match(r'\[(東|西|地|外)\](.*)', value)
                            if match:
                                result['所属'] = match.group(1)
                                result['調教師'] = match.group(2).strip()
                            else:
                                result['所属'] = ''
                                result['調教師'] = value
                        else:
                            result['所属'] = ''
                            result['調教師'] = ''
                    
                    # 賞金カラムの処理
                    elif header == '賞金(万円)':
                        try:
                            value = float(value.replace(',', '')) * 10000 if value else 0.0
                            result['賞金'] = value
                        except ValueError:
                            result['賞金'] = 0.0
                    
                    # オッズの処理
                    elif header == 'オッズ':
                        try:
                            result[header] = float(value)
                        except (ValueError, TypeError):
                            result[header] = -1.0
                    
                    # その他のカラム
                    else:
                        result[header] = value
            
            results.append(result)
        
        return results

    def get_existing_race_ids(self, target_year=None):
        """
        GCSの既存のCSVファイルから取得済みのレースIDを取得
        Args:
            target_year (int, optional): 指定された年のレースIDのみを取得
        """
        existing_ids = set()
        
        try:
            # race_info_formatted.csvの確認
            info_bucket = self.storage_client.bucket('nk_race_info')
            info_blob = info_bucket.blob('race_info_formatted.csv')
            
            if info_blob.exists():
                with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
                    info_blob.download_to_file(temp_file)
                df_info = pd.read_csv(temp_file.name)
                if 'race_id' in df_info.columns:
                    df_info['race_id'] = df_info['race_id'].astype(str)
                    if target_year:
                        df_info = df_info[df_info['race_id'].str[:4] == str(target_year)]
                    existing_ids.update(df_info['race_id'])
                os.unlink(temp_file.name)
            
            # race_result_formatted.csvの確認
            result_bucket = self.storage_client.bucket('nk_race_result')
            result_blob = result_bucket.blob('race_result_formatted.csv')
            
            if result_blob.exists():
                with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
                    result_blob.download_to_file(temp_file)
                df_result = pd.read_csv(temp_file.name)
                if 'race_id' in df_result.columns:
                    df_result['race_id'] = df_result['race_id'].astype(str)
                    if target_year:
                        df_result = df_result[df_result['race_id'].str[:4] == str(target_year)]
                    existing_ids.update(df_result['race_id'])
                os.unlink(temp_file.name)
            
        except Exception as e:
            print(f"Error reading existing race IDs from GCS: {e}")
        
        return existing_ids

    def get_last_processed_position(self):
        """
        GCSから最後に処理したレースの位置を特定（race_infoとrace_result両方に存在する最後のレースIDを取得）
        Returns:
            tuple: (year, place_code, kai, day) または None
        """
        info_race_ids = set()
        result_race_ids = set()
        
        try:
            # race_info_formatted.csvの確認
            info_bucket = self.storage_client.bucket('nk_race_info')
            info_blob = info_bucket.blob('race_info_formatted.csv')
            
            if info_blob.exists():
                with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
                    info_blob.download_to_file(temp_file)
                df_info = pd.read_csv(temp_file.name)
                if 'race_id' in df_info.columns and not df_info['race_id'].empty:
                    info_race_ids = set(df_info['race_id'].astype(str))
                os.unlink(temp_file.name)
            
            # race_result_formatted.csvの確認
            result_bucket = self.storage_client.bucket('nk_race_result')
            result_blob = result_bucket.blob('race_result_formatted.csv')
            
            if result_blob.exists():
                with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
                    result_blob.download_to_file(temp_file)
                df_result = pd.read_csv(temp_file.name)
                if 'race_id' in df_result.columns and not df_result['race_id'].empty:
                    result_race_ids = set(df_result['race_id'].astype(str))
                os.unlink(temp_file.name)
            
            # 両方のファイルに存在するレースIDを取得
            common_race_ids = info_race_ids.intersection(result_race_ids)
            
            if common_race_ids:
                # 最大のレースIDを取得
                last_race_id = max(common_race_ids)
                
                # レースIDを分解（例: 202401010102 → 2024, 01, 01, 01）
                year = last_race_id[:4]
                place = last_race_id[4:6]
                kai = last_race_id[6:8]
                day = last_race_id[8:10]
                return int(year), place, int(kai), int(day)
            
        except Exception as e:
            print(f"Error getting last processed position from GCS: {e}")
        
        return None

    def save_consolidated_csv(self, race_infos, race_results):
        """
        レース情報と結果をGCSに保存（既存データとマージ）
        """
        try:
            # レース情報の保存
            if race_infos:
                info_bucket = self.storage_client.bucket('nk_race_info')
                info_blob = info_bucket.blob('race_info_formatted.csv')
                
                df_info = pd.DataFrame(race_infos)
                df_info['race_id'] = df_info['race_id'].astype(str)
                
                # 既存のファイルが存在する場合は読み込んでマージ
                if info_blob.exists():
                    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
                        info_blob.download_to_file(temp_file)
                    existing_df = pd.read_csv(temp_file.name)
                    existing_df['race_id'] = existing_df['race_id'].astype(str)
                    
                    # 既存のデータを削除して新しいデータで上書き
                    existing_df = existing_df[~existing_df['race_id'].isin(df_info['race_id'])]
                    df_info = pd.concat([existing_df, df_info], ignore_index=True)
                    os.unlink(temp_file.name)
                
                # 一時ファイルに保存してアップロード
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                    df_info.to_csv(temp_file.name, index=False)
                    info_blob.upload_from_filename(temp_file.name)
                    os.unlink(temp_file.name)
                
                print(f"Saved {len(race_infos)} race info records")
            
            # レース結果の保存
            if race_results:
                result_bucket = self.storage_client.bucket('nk_race_result')
                result_blob = result_bucket.blob('race_result_formatted.csv')
                
                df_results = pd.DataFrame(race_results)
                df_results['race_id'] = df_results['race_id'].astype(str)
                
                # 既存のファイルが存在する場合は読み込んでマージ
                if result_blob.exists():
                    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
                        result_blob.download_to_file(temp_file)
                    existing_df = pd.read_csv(temp_file.name)
                    existing_df['race_id'] = existing_df['race_id'].astype(str)
                    
                    # 既存のデータを削除して新しいデータで上書き
                    if '馬番' in df_results.columns:
                        existing_df = existing_df[~((existing_df['race_id'].isin(df_results['race_id'])) & 
                                                  (existing_df['馬番'].isin(df_results['馬番'])))]
                    else:
                        existing_df = existing_df[~existing_df['race_id'].isin(df_results['race_id'])]
                    
                    df_results = pd.concat([existing_df, df_results], ignore_index=True)
                    os.unlink(temp_file.name)
                
                # 一時ファイルに保存してアップロード
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
                    df_results.to_csv(temp_file.name, index=False)
                    result_blob.upload_from_filename(temp_file.name)
                    os.unlink(temp_file.name)
                
                print(f"Saved {len(race_results)} race result records")
            
        except Exception as e:
            print(f"Error saving to GCS: {str(e)}")
            raise

    def process_races(self, year=None, place=None, kai=None, day=None):
        """
        指定された条件でレースをスクレイピング
        Args:
            year (int, optional): 年。指定がない場合は2024年から現在年まで処理
            place (str, optional): 競馬場コード
            kai (int, optional): 開催回
            day (int, optional): 開催日
        Returns:
            dict: スクレイピング結果のステータス
        """
        start_time = time.time()
        try:
            print("Starting race processing...")
            
            # 既存のレースIDを取得（指定された年のみ）
            print("Fetching existing race IDs...")
            fetch_start = time.time()
            existing_race_ids = self.get_existing_race_ids(year)
            print(f"Fetched existing race IDs in {time.time() - fetch_start:.2f} seconds")
            
            # 最後に処理した位置を取得
            last_position = self.get_last_processed_position()
            if last_position:
                last_year, last_place, last_kai, last_day = last_position
                print(f"Last processed position: Year={last_year}, Place={last_place}, Kai={last_kai}, Day={last_day}")
            
            # 年の設定
            current_year = datetime.now().year
            if year is None:
                start_year = 2024  # 2024年から開始
                end_year = current_year
            else:
                start_year = end_year = year
            
            print(f"Processing races from {start_year} to {end_year}")
            
            # 特定の日付が指定されている場合
            if all([year, place, kai, day]):
                result = self._process_specific_date(year, place, kai, day, existing_race_ids)
            else:
                # 年間データを処理
                for year in range(start_year, end_year + 1):
                    result = self._process_yearly_data(year, existing_race_ids, last_position)
                    if result['status'] == 'error':
                        return result
            
            total_time = time.time() - start_time
            print(f"\nTotal processing time: {total_time:.2f} seconds")
            return {'status': 'success', 'message': f'Processed all races from {start_year} to {end_year}'}
            
        except Exception as e:
            error_message = str(e)
            print(f"Error in process_races: {error_message}")
            return {'status': 'error', 'message': error_message}

    def _process_specific_date(self, year, place, kai, day, existing_race_ids):
        """特定の日付のレースを処理"""
        base_race_id = f"{year}{place}{kai:02d}{day:02d}"
        race_infos = []
        race_results = []
        
        for race_num in range(1, 13):
            race_id = f"{base_race_id}{race_num:02d}"
            
            # 未取得のレースのみ処理
            if race_id not in existing_race_ids:
                race_data = self.scrape_race_result(race_id)
                
                if race_data:
                    if race_data['race_info']:
                        race_data['race_info']['race_id'] = race_id
                        race_infos.append(race_data['race_info'])
                    
                    if race_data['race_results']:
                        for result in race_data['race_results']:
                            result['race_id'] = race_id
                        race_results.extend(race_data['race_results'])
                
                time.sleep(1)  # レート制限対策
        
        if race_infos or race_results:
            self.save_consolidated_csv(race_infos, race_results)
        
        return {'status': 'success', 'message': f'Processed races for {base_race_id}'}

    def _process_yearly_data(self, year, existing_race_ids, last_position):
        """年間データを処理"""
        year_start_time = time.time()
        place_codes = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10']
        start_kai = 1
        start_day = 1
        start_race = 1
        
        # race_infoとrace_resultの既存データを個別に取得
        print("\nFetching existing race data...")
        fetch_start = time.time()
        info_race_ids = set()
        result_race_ids = set()
        
        try:
            # race_info_formatted.csvの確認
            info_bucket = self.storage_client.bucket('nk_race_info')
            info_blob = info_bucket.blob('race_info_formatted.csv')
            
            if info_blob.exists():
                with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
                    info_blob.download_to_file(temp_file)
                df_info = pd.read_csv(temp_file.name)
                if 'race_id' in df_info.columns:
                    df_info['race_id'] = df_info['race_id'].astype(str)
                    info_race_ids = set(df_info[df_info['race_id'].str[:4] == str(year)]['race_id'])
                os.unlink(temp_file.name)
            
            # race_result_formatted.csvの確認
            result_bucket = self.storage_client.bucket('nk_race_result')
            result_blob = result_bucket.blob('race_result_formatted.csv')
            
            if result_blob.exists():
                with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
                    result_blob.download_to_file(temp_file)
                df_result = pd.read_csv(temp_file.name)
                if 'race_id' in df_result.columns:
                    df_result['race_id'] = df_result['race_id'].astype(str)
                    result_race_ids = set(df_result[df_result['race_id'].str[:4] == str(year)]['race_id'])
                os.unlink(temp_file.name)
        
        except Exception as e:
            print(f"Error reading existing race IDs: {e}")
            info_race_ids = set()
            result_race_ids = set()
        
        print(f"Fetched existing race data in {time.time() - fetch_start:.2f} seconds")
        
        # 片方にしか存在しないレースIDを特定
        info_only_ids = info_race_ids - result_race_ids
        result_only_ids = result_race_ids - info_race_ids
        
        if info_only_ids or result_only_ids:
            print(f"Found {len(info_only_ids)} races with only race_info and {len(result_only_ids)} races with only race_result")
        
        jst_now = datetime.now(timezone(timedelta(hours=9)))
        print(f"\n[{jst_now.strftime('%Y-%m-%d %H:%M:%S')} JST] Processing races for {year}年")
        
        total_requests = 0
        total_races_processed = 0
        
        for place in place_codes:
            place_start_time = time.time()
            race_infos = []
            race_results = []
            
            # 開始位置の設定
            current_kai = start_kai
            for kai in range(current_kai, 13):
                current_day = start_day if kai == start_kai else 1
                for day in range(current_day, 21):
                    base_race_id = f"{year}{place}{kai:02d}{day:02d}"
                    first_race_id = f"{base_race_id}01"
                    
                    try:
                        request_start = time.time()
                        response = requests.get(f'https://db.netkeiba.com/race/{first_race_id}', headers=self.headers)
                        total_requests += 1
                        time.sleep(1)
                        
                        if response.status_code == 200:
                            soup = BeautifulSoup(response.text, 'html.parser')
                            if soup.select_one('.race_table_01'):
                                print(f"Processing races for {base_race_id} (Request took {time.time() - request_start:.2f}s)")
                                
                                current_race = start_race if kai == start_kai and day == start_day else 1
                                for race_num in range(current_race, 13):
                                    race_id = f"{base_race_id}{race_num:02d}"
                                    
                                    # レースIDが存在しない場合、または片方のファイルにしか存在しない場合にスクレイピング
                                    if race_id not in existing_race_ids or race_id in info_only_ids or race_id in result_only_ids:
                                        race_start = time.time()
                                        race_data = self.scrape_race_result(race_id)
                                        total_requests += 1
                                        
                                        if race_data:
                                            total_races_processed += 1
                                            # race_infoの処理
                                            if race_data['race_info'] and (race_id not in info_race_ids or race_id in result_only_ids):
                                                race_info = race_data['race_info']
                                                race_info['race_id'] = race_id
                                                race_infos.append(race_info)
                                            
                                            # race_resultの処理
                                            if race_data['race_results'] and (race_id not in result_race_ids or race_id in info_only_ids):
                                                for result in race_data['race_results']:
                                                    result['race_id'] = race_id
                                                race_results.extend(race_data['race_results'])
                                            
                                            # データを保存
                                            if race_infos or race_results:
                                                save_start = time.time()
                                                self.save_consolidated_csv(race_infos, race_results)
                                                print(f"Saved data in {time.time() - save_start:.2f} seconds")
                                                race_infos = []
                                                race_results = []
                                        
                                        print(f"Processed race {race_id} in {time.time() - race_start:.2f} seconds")
                                        time.sleep(3)
                
                    except Exception as e:
                        print(f"Error checking {first_race_id}: {str(e)}")
                        continue
                
                # 最初の日以降は通常の開始位置から
                start_day = 1
            
            # 最初の開催回以降は通常の開始位置から
            start_kai = 1
            start_race = 1
            
            print(f"\nCompleted processing place {place} in {time.time() - place_start_time:.2f} seconds")
            time.sleep(30)  # 競馬場間に30秒待機
        
        total_time = time.time() - year_start_time
        print(f"\nYear {year} processing completed:")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Total requests made: {total_requests}")
        print(f"Total races processed: {total_races_processed}")
        print(f"Average time per race: {total_time/total_races_processed:.2f} seconds (if races were processed)")
        
        return {'status': 'success', 'message': f'Processed all races for {year}'}

# Cloud Functions用のエントリーポイント
@functions_framework.http
def scrape_races(request):
    """HTTP Cloud Functions用のエントリーポイント"""
    try:
        request_json = request.get_json(silent=True)
        
        year = request_json.get('year') if request_json else None
        place = request_json.get('place') if request_json else None
        kai = request_json.get('kai') if request_json else None
        day = request_json.get('day') if request_json else None
        
        scraper = NetkeibaRaceScraper()
        result = scraper.process_races(year, place, kai, day)
        
        return result
        
    except Exception as e:
        error_message = str(e)
        print(f"Error in scrape_races: {error_message}")
        return {'status': 'error', 'message': error_message}, 500

# ローカル実行用のエントリーポイント
if __name__ == "__main__":
    scraper = NetkeibaRaceScraper()
    result = scraper.process_races()
    print(f"Scraping completed with status: {result['status']}")
    if result['status'] == 'error':
        print(f"Error message: {result['message']}")