# Rotowire Endpoint Probe Results

Generated: 2026-02-18T15:02:36.641275

## RSS Feed

- URL: `https://www.rotowire.com/rss/news.php?sport=nhl`
- Items found: 5
- Guid range: nhl582946 .. nhl582960

## /hockey/ajax/get-more-updates.php

| # | Request | Status | News? |
|---|---------|--------|-------|
| 1 | GET ?id=582946 | 200 | no |
| 2 | GET ?id=nhl582946 | 200 | no |
| 3 | GET ?ID=582946 (capital) | 200 | no |
| 4 | POST id=582946 | 200 | no |
| 5 | POST id=nhl582946 | 200 | no |
| 6 | GET ?id=582946&sport=nhl | 200 | no |
| 7 | GET ?id=0 | 200 | no |
| 8 | GET (no id, baseline) | 200 | no |
| 9 | GET ?id=582946 with Accept: application/json | 200 | no |
| 10 | GET ?id=582946 with Accept: text/html | 200 | no |
| 11 | GET ?id=582946 with Accept: */* | 200 | no |
| 12 | GET ?id=582946 with NO Referer | 200 | no |

### Accept Header Sensitivity

Baseline (no explicit Accept): status=200, response length=35 chars

- GET ?id=582946 with Accept: application/json: status=200, length=35 chars (SAME as baseline)
- GET ?id=582946 with Accept: text/html: status=200, length=35 chars (SAME as baseline)
- GET ?id=582946 with Accept: */*: status=200, length=35 chars (SAME as baseline)

### Referer Enforcement

Removing the Referer header had **no effect** (status 200, same as with Referer).

## /frontend/ajax/get-articles.php

| # | Request | Status | News? | JSON Keys |
|---|---------|--------|-------|-----------|
| 1 | ?sport=hockey | 200 | no | success, articlesHTML, lastArticleDate, noMoreArticles |
| 2 | ?sport=nhl | 200 | YES | success, articlesHTML, lastArticleDate, noMoreArticles |
| 3 | ?sport=hockey&page=1 | 200 | no | success, articlesHTML, lastArticleDate, noMoreArticles |
| 4 | ?sport=hockey&lastArticleDate=2026-02-01 | 200 | no | success, articlesHTML, lastArticleDate, noMoreArticles |
| 5 | ?category=hockey&type=news | 200 | YES | success, articlesHTML, lastArticleDate, noMoreArticles |

## Pagination Analysis

- Source: `articles`
- Winning request: ?sport=nhl
- Items per page: 45
- Pagination via: `lastArticleDate=2026-02-05`
- Page 2 status: 200
- Page 2 HTML length: 22742 chars
- Response JSON keys: ['success', 'articlesHTML', 'lastArticleDate', 'noMoreArticles']

## Sample Response

```json
{"success":true,"articlesHTML":"\r\n\t<div class=\"content-preview is-traditional \" data-articleid=\"105554\" data-article-date=\"2026-02-17\" style=\"order:317\">\r\n\t\t<div class=\"content-preview__top-container\" >\t\r\n\t\t\t<a href=\"\/hockey\/article\/winter-2026-olympics-mens-hockey-gold-odds-picks-105554\" class=\"content-preview__image-box\"><img class=\"lozad \" style=\"aspect-ratio:6 \/ 5\" data-src=\"https:\/\/res.cloudinary.com\/rotowire\/image\/upload\/ar_6:5,c_fill,dpr_auto,f_auto,g_auto:custom_no_override,q_auto,w_auto\/er6zneoxxzgfxqaquogq.jpg\" alt=\"2026 Winter Olympics Men's Hockey Quarterfinals: Gold Medal Odds & Expert Picks\" loading=\"lazy\" ><\/a>\r\n\t\t\t<div class=\"content-preview__top-content\" > \r\n\t\t\t\t<div class=\"content-preview__sport\">NHL<\/div>\r\n\t\t\t\t<div class=\"content-preview__title-wrapper\">\r\n\t\t\t\t\t<a href=\"\/hockey\/article\/winter-2026-olympics-mens-hockey-gold-odds-picks-105554\" class=\"content-preview__title\">2026 Winter Olympics Men's Hockey Quarterfinals: Gold Medal Odds & Expert Picks<\/a>\r\n\t\t\t\t<\/div>\r\n\t\t\t<\/div>\r\n\t\t<\/div>\r\n\t\t\t<div class=\"content-preview__bottom-content\" >\r\n\t\t\t\t<div class=\"content-preview__author\" ><div class=\"article__author\" ><div class=\"article__author-image\"><img class=\"\" src=\"https:\/\/content.rotowire.com\/images\/photos\/Dobish-Daniel.png\" alt=\"Author Image\"  onerror=\"this.onerror=null; this.src='https:\/\/content.rotowire.com\/images\/icons
... (truncated)
```

## Recommendation

**Use the `articles` endpoint** for paginated news:
- URL: `https://www.rotowire.com/frontend/ajax/get-articles.php`
- Method: GET
- Required headers: User-Agent, X-Requested-With: XMLHttpRequest
- Pagination: pass `lastArticleDate` from response as cursor
- Items per page: ~45

