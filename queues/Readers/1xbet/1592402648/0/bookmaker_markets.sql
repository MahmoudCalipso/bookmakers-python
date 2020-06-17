INSERT INTO bookmaker_markets (id, fk_bookmaker_id, fk_bookmaker_sport_id, title, skip, outcomes, mapped, created_at) VALUES 
(DEFAULT, 14, {sport=Cricket}, 'Double chance', 0, '["1X", "12", "2X"]', 0, '2020-06-17 00:00:00')
,(DEFAULT, 14, {sport=Formula 1}, 'Driver To Win', 0, '["Red Bull", "Mercedes", "Ferrari", "McLaren", "Williams", "Renault", "Haas", "Alfa Romeo", "Racing Point", "Alpha Tauri", "Red Bull", "Mercedes", "Ferrari"]', 0, '2020-06-17 00:00:00')
ON CONFLICT (fk_bookmaker_id, fk_bookmaker_sport_id, title) DO UPDATE SET outcomes = EXCLUDED.outcomes;