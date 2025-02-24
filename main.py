from scraping.scraping_netkeiba import NetkeibaRaceScraper

if __name__ == "__main__":
    # main()
    scraper = NetkeibaRaceScraper()
    result = scraper.process_races(year=2025)
    print(f"\nスクレイピング完了")
    print(f"ステータス: {result['status']}")
    print(f"メッセージ: {result['message']}")
