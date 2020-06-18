INSERT INTO bookmaker_tournaments (id, fk_bookmaker_id, fk_bookmaker_sport_id, title, skip, mapped, created_at, found_in) VALUES 
(DEFAULT, 14, {sport=Football}, 'England. Championship', 0, 0, '2020-06-18 00:00:00', 'Millwall vs Derby County')
ON CONFLICT (fk_bookmaker_id, fk_bookmaker_sport_id, title) DO NOTHING;