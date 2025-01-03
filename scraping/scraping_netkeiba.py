import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import configparser
import os
import re
import time

class NetkeibaRaceScraper:
    def __init__(self):
        self.config = self._load_config()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Cookie': 'uid=0; nkauth=0'
        }

    def _load_config(self):
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config.ini')
        config.read(config_path, encoding='utf-8')
        return config

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
                race_info['race_date'] = parts[0]
                
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
                if '芝' in line and 'm' in line and '天候' in line:
                    info_parts = line.split('/')
                    
                    # コース情報を分割
                    if len(info_parts) > 0:
                        course_info = info_parts[0].strip()
                        # 例: "芝右 外2200m" → ["芝右", "外2200m"]
                        course_parts = course_info.split()
                        
                        # 馬場の種類（芝/ダ）と回り（右/左/直線）を分離
                        if len(course_parts) > 0:
                            track_type = course_parts[0]
                            race_info['track_type'] = '芝' if '芝' in track_type else 'ダ'
                            race_info['track_direction'] = '右' if '右' in track_type else ('左' if '左' in track_type else '直線')
                        
                        # 内外とコース距離を分離
                        if len(course_parts) > 1:
                            distance_part = course_parts[1]
                            # 内外の情報を抽出
                            if '内' in distance_part:
                                race_info['track_inout'] = '内'
                                distance_part = distance_part.replace('内', '')
                            elif '外' in distance_part:
                                race_info['track_inout'] = '外'
                                distance_part = distance_part.replace('外', '')
                            else:
                                race_info['track_inout'] = ''
                            
                            # 距離を抽出（mを除去）
                            race_info['track_distance'] = distance_part.replace('m', '')
                    
                    # 天候、馬場状態、発走時刻
                    for part in info_parts:
                        part = part.strip()
                        if '天候' in part:
                            race_info['weather'] = part.split(':')[1].strip()
                        elif '芝' in part and ':' in part:
                            race_info['track_condition'] = part.split(':')[1].strip()
                        elif '発走' in part:
                            time_parts = part.split(':')
                            if len(time_parts) >= 2:
                                race_info['start_time'] = ':'.join(time_parts[1:]).strip()
                    break
        
        # タイムスタンプの生成
        if 'race_date' in race_info and 'start_time' in race_info:
            try:
                date_str = race_info['race_date'].replace('年', '-').replace('月', '-').replace('日', '')
                time_str = race_info['start_time']
                
                if ':' not in time_str:
                    time_str = f"{time_str[:2]}:{time_str[2:]}"
                
                datetime_str = f"{date_str} {time_str}"
                race_info['timestamp'] = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
                
            except ValueError as e:
                print(f"Error creating timestamp: {e}")
        
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
                    
                    # タイムの処理
                    if header == 'タイム':
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
                    
                    # その他のカラム
                    else:
                        result[header] = value
            
            results.append(result)
        
        return results

    def save_to_csv(self, race_data, results_path, info_path):
        """結果をCSVファイルに保存"""
        try:
            if not race_data:
                print("No race data to save")
                return
            
            # レース結果の保存
            if race_data['race_results']:
                df_results = pd.DataFrame(race_data['race_results'])
                
                # race_idカラムを追加
                race_id = os.path.splitext(os.path.basename(results_path))[0].replace('race_result_', '')
                df_results['race_id'] = race_id
                
                # カラムの順序を指定（race_idを先頭に）
                cols = ['race_id'] + [col for col in df_results.columns if col != 'race_id']
                df_results = df_results[cols]
                
                output_dir = os.path.dirname(results_path)
                if output_dir and not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                df_results.to_csv(results_path, index=False, encoding='utf-8-sig')
                print(f"Race results saved to {results_path}")
            
            # レース情報の保存
            if race_data['race_info']:
                # カラムの順序を指定（race_idを先頭に追加）
                columns = [
                    'race_id',  # 追加
                    'race_name', 'race_date', 
                    'kaisai_kai', 'kaisai_place', 'kaisai_nichime',
                    'track_type', 'track_direction', 'track_inout', 'track_distance',
                    'weather', 'track_condition', 'start_time',
                    'race_conditions', 'timestamp'
                ]
                
                df_info = pd.DataFrame([race_data['race_info']])
                
                # race_idカラムを追加
                race_id = os.path.splitext(os.path.basename(info_path))[0].replace('race_info_', '')
                df_info['race_id'] = race_id
                
                existing_columns = [col for col in columns if col in df_info.columns]
                df_info = df_info[existing_columns]
                
                if 'timestamp' in df_info.columns:
                    df_info['timestamp'] = pd.to_datetime(df_info['timestamp'])
                
                output_dir = os.path.dirname(info_path)
                if output_dir and not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                
                for col in df_info.columns:
                    if df_info[col].dtype == 'object':
                        df_info[col] = df_info[col].str.replace('\n', '').str.strip()
                
                df_info.to_csv(info_path, index=False, encoding='utf-8-sig')
                print(f"Race information saved to {info_path}")
            
        except Exception as e:
            print(f"Error saving to CSV: {str(e)}")

    def scrape_multiple_races(self, start_race_id):
        """
        指定されたレースIDから順番にスクレイピングを行う
        Args:
            start_race_id (str): 開始レースID (例: "202406")
        """
        base_id = start_race_id
        race_infos = []
        race_results = []
        
        # 1R から 12R まで
        for race_num in range(1, 13):
            race_id = f"{base_id}{str(race_num).zfill(2)}"
            print(f"Scraping race ID: {race_id}")
            
            race_data = self.scrape_race_result(race_id)
            if race_data:
                if race_data['race_info']:
                    race_data['race_info']['race_id'] = race_id
                    race_infos.append(race_data['race_info'])
                
                if race_data['race_results']:
                    for result in race_data['race_results']:
                        result['race_id'] = race_id
                    race_results.extend(race_data['race_results'])
        
        return race_infos, race_results

    def save_consolidated_csv(self, race_infos, race_results, output_dir):
        """
        レース情報と結果を1つのCSVファイルにまとめて保存
        """
        try:
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # レース情報の保存
            if race_infos:
                columns = [
                    'race_id', 'race_name', 'race_date',
                    'kaisai_kai', 'kaisai_place', 'kaisai_nichime',
                    'track_type', 'track_direction', 'track_inout', 'track_distance',
                    'weather', 'track_condition', 'start_time',
                    'race_conditions', 'timestamp'
                ]
                
                df_info = pd.DataFrame(race_infos)
                existing_columns = [col for col in columns if col in df_info.columns]
                df_info = df_info[existing_columns]
                
                if 'timestamp' in df_info.columns:
                    df_info['timestamp'] = pd.to_datetime(df_info['timestamp'])
                
                info_path = os.path.join(output_dir, 'race_info_consolidated.csv')
                
                # 既存のファイルがある場合は追記
                if os.path.exists(info_path):
                    existing_df = pd.read_csv(info_path)
                    df_info = pd.concat([existing_df, df_info], ignore_index=True)
                    # race_idでユニークにする
                    df_info = df_info.drop_duplicates(subset=['race_id'], keep='last')
                
                df_info.to_csv(info_path, index=False, encoding='utf-8-sig')
                print(f"Race information saved to {info_path}")
            
            # レース結果の保存
            if race_results:
                df_results = pd.DataFrame(race_results)
                
                # race_idを先頭に
                cols = ['race_id'] + [col for col in df_results.columns if col != 'race_id']
                df_results = df_results[cols]
                
                results_path = os.path.join(output_dir, 'race_result_consolidated.csv')
                
                # 既存のファイルがある場合は追記
                if os.path.exists(results_path):
                    existing_df = pd.read_csv(results_path)
                    df_results = pd.concat([existing_df, df_results], ignore_index=True)
                    # race_idと馬番でユニークにする
                    if '馬番' in df_results.columns:
                        df_results = df_results.drop_duplicates(subset=['race_id', '馬番'], keep='last')
                
                df_results.to_csv(results_path, index=False, encoding='utf-8-sig')
                print(f"Race results saved to {results_path}")
            
        except Exception as e:
            print(f"Error saving consolidated CSV: {str(e)}")

    def get_race_ids_for_date(self, base_id):
        """
        指定された日付のレースID一覧を取得
        Args:
            base_id (str): 基準となるレースID (例: "202406")
        Returns:
            list: レースID一覧
        """
        race_ids = []
        
        # 開催場所のコード（主要な競馬場）
        place_codes = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10']
        
        # 各開催場所の各開催日をチェック
        for place in place_codes:
            for day in range(1, 9):  # 通常1-8日までの開催
                # レースIDの構成: YYYYMM + 場所コード + 開催日 + レース番号
                base_race_id = f"{base_id}{place}{day:02d}"
                
                # まず1Rをチェックして開催があるか確認
                first_race_id = f"{base_race_id}01"
                url = f'https://db.netkeiba.com/race/{first_race_id}'
                
                try:
                    response = requests.get(url, headers=self.headers)
                    time.sleep(1)  # 1秒待機
                    
                    if response.status_code == 200:
                        # レース結果ページが存在するか確認
                        soup = BeautifulSoup(response.text, 'html.parser')
                        if soup.select_one('.race_table_01'):
                            print(f"Found races for {base_race_id}")
                            # その日の全レースを追加（1R-12R）
                            for race_num in range(1, 13):
                                race_id = f"{base_race_id}{race_num:02d}"
                                race_ids.append(race_id)
                                print(f"Added race: {race_id}")
                    
                except Exception as e:
                    print(f"Error checking {first_race_id}: {str(e)}")
                    continue
        
        return race_ids

def main():
    scraper = NetkeibaRaceScraper()
    
    # 出力パスの設定
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'results')
    
    # 2015年から2025年まで
    for year in range(2015, 2026):
        # 1月から12月まで
        for month in range(1, 13):
            base_id = f"{year}{month:02d}"
            print(f"\nStarting scraping for {year}年{month}月 (ID: {base_id})")
            
            # 対象月の全レースIDを取得
            race_ids = scraper.get_race_ids_for_date(base_id)
            print(f"Found {len(race_ids)} races")
            
            if not race_ids:
                print(f"No races found for {year}年{month}月")
                continue
            
            race_infos = []
            race_results = []
            
            # 各レースをスクレイピング
            for race_id in race_ids:
                print(f"Scraping race ID: {race_id}")
                race_data = scraper.scrape_race_result(race_id)
                
                if race_data:
                    if race_data['race_info']:
                        race_info = race_data['race_info']
                        race_info['race_id'] = race_id
                        race_infos.append(race_info)
                        
                        # レース結果にレース情報を追加
                        if race_data['race_results']:
                            for result in race_data['race_results']:
                                result['race_id'] = race_id
                                # レース情報をコピー
                                for key in ['race_name', 'race_date', 'kaisai_kai', 'kaisai_place', 
                                          'kaisai_nichime', 'track_type', 'track_direction', 
                                          'track_inout', 'track_distance', 'weather', 
                                          'track_condition', 'start_time', 'race_conditions']:
                                    if key in race_info:
                                        result[key] = race_info[key]
                            race_results.extend(race_data['race_results'])
                
                # レース間に3秒待機
                time.sleep(3)
            
            if race_infos or race_results:
                scraper.save_consolidated_csv(race_infos, race_results, output_dir)
                print(f"Saved data for {year}年{month}月")
            
            # 月間の処理が終わるごとに30秒待機
            print(f"Waiting 30 seconds before processing next month...")
            time.sleep(30)

if __name__ == "__main__":
    main()