INSERT INTO bookmaker_teams (id, fk_bookmaker_id, fk_bookmaker_sport_id, title, found_in, ignore, mapped, created_at) VALUES 
(DEFAULT, 14, {sport=Football}, 'Birmingham City', 'West Bromwich Albion vs Birmingham City', 0, 0, '2020-06-18 00:00:00')
ON CONFLICT (fk_bookmaker_id, fk_bookmaker_sport_id, title) DO NOTHING;