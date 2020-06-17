INSERT INTO bookmaker_sports (id, fk_bookmaker_id, title, ignore, mapped, created_at) VALUES 
(DEFAULT, 14, 'Pesäpallo', 0, 0, '2020-06-17 00:00:00')
ON CONFLICT (fk_bookmaker_id, title) DO NOTHING;