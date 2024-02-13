SELECT source_name, COUNT(*) as article_count
FROM news_data_table
GROUP BY source_name
ORDER BY article_count DESC;


SELECT published_at, COUNT(*) as total_articles
FROM news_data_table
GROUP BY published_at;

SELECT keyword, COUNT(*) as frequency
FROM news_data_table
GROUP BY keyword
ORDER BY frequency DESC;

SELECT day_of_week, COUNT(*) as total_articles
FROM news_data_table
GROUP BY day_of_week
ORDER BY total_articles DESC;

SELECT author, COUNT(*) as total_articles
FROM news_data_table
WHERE author IS NOT NULL AND author != ''
GROUP BY author
ORDER BY total_articles DESC;