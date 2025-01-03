import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import configparser
import os

class NetkeibaRaceScraper:
    def __init__(self):
        self.config = self._load_config()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
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
                'race_name': self._get_race_name(soup),
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

    def _get_race_name(self, soup):
        """レース名を取得"""
        race_name = soup.select_one('.RaceName')
        if not race_name:
            race_name = soup.select_one('.race_name')
        return race_name.text.strip() if race_name else None

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
                    
                    # タイムの処理
                    if headers[i] == 'タイム':
                        value = self._convert_time_to_seconds(value)
                    
                    # 通過順位の処理
                    elif headers[i] == '通過':
                        positions = value.split('-')
                        for j in range(4):
                            column_name = f'通過_{j+1}F'
                            result[column_name] = positions[j] if j < len(positions) else ''
                        continue
                    
                    # 賞金カラムの処理
                    elif headers[i] == '賞金(万円)':
                        try:
                            value = float(value.replace(',', '')) * 10000 if value else 0.0
                        except ValueError:
                            value = 0.0
                    
                    result[headers[i]] = value
            results.append(result)
        
        return results

    def save_to_csv(self, race_data, output_path):
        """結果をCSVファイルに保存"""
        try:
            if not race_data:
                print("No race data to save")
                return
                
            if not race_data['race_results']:
                print("No race results to save")
                return
                
            df = pd.DataFrame(race_data['race_results'])
            
            # 出力ディレクトリの作成
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"Results successfully saved to {output_path}")
            
        except Exception as e:
            print(f"Error saving results to CSV: {str(e)}")

def main():
    scraper = NetkeibaRaceScraper()
    race_id = "202408060411"
    
    # スクリプトのディレクトリを基準とした出力パスの設定
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'results')
    output_path = os.path.join(output_dir, f'race_result_{race_id}.csv')
    
    print(f"Starting scraping for race ID: {race_id}")
    race_data = scraper.scrape_race_result(race_id)
    
    if race_data:
        scraper.save_to_csv(race_data, output_path)
    else:
        print("Failed to get race data")

if __name__ == "__main__":
    main()