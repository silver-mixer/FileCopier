# データベース仕様書

## テーブル一覧

| テーブル名 | 概要 |
| --- | --- |
| transferfiles | ファイル情報テーブル |
| information | DB情報テーブル |

## transferfilesテーブル
コピーしたファイル情報を保持する。

| No | カラム名 | 項目名 | データ型 | Not Null | PK | 制約 | 概要 |
| --- | --- | --- | --- | :---: | :---: | --- | --- |
| 1 | id | ID | INTEGER | Y | Y | AUTOINCREMENT | 通し番号 |
| 2 | hash | ファイルハッシュ | TEXT | Y | - | | ファイルのMD5ハッシュ値 |
| 3 | size | ファイルサイズ | INTEGER | Y | - | | ファイルのサイズ(バイト単位) |
| 4 | last_modified | ファイル最終更新日 | INTEGER | Y | - | | ファイルの最終更新日のUNIX時間(ミリ秒単位) |
| 5 | source | コピー元パス | TEXT | Y | - | | ファイルのコピー元パス |
| 6 | destination | コピー先パス | TEXT | Y | - | | ファイルのコピー先パス |

## informationテーブル
DBの情報を保持する。

| No | カラム名 | 項目名 | データ型 | Not Null | PK | 制約 | 概要 |
| --- | --- | --- | --- | :---: | :---: | --- | --- |
| 1 | key | キー | TEXT | Y | Y | | キー |
| 2 | value | 値 | TEXT | - | - | | 値 |

### Key-Value
informationテーブルで保持されるKey-Valueは次の通り。

| No | キー | 値 | 概要 |
| --- | --- | --- | --- |
| 1 | version | 1 | DBのバージョン(固定値) |
| 2 | updated_at | - | 最終更新日時のUNIX時間(ミリ秒単位) |
