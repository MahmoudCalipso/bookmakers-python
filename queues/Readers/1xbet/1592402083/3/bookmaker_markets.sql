INSERT INTO bookmaker_markets (id, fk_bookmaker_id, fk_bookmaker_sport_id, title, skip, outcomes, mapped, created_at) VALUES 
(DEFAULT, 14, {sport=Pesäpallo}, '1X2', 0, '["W1", "X", "W2"]', 0, '2020-06-17 00:00:00')
,(DEFAULT, 14, {sport=Pesäpallo}, 'Who will win - Yes/No', 0, '["Vimpeli", "Sotkamo", "Pattijoki Raahe", "Hyvinkaa", "Kankaanpaa", "Kitee", "Koskenkorva", "Imatran Pallo-Veikot", "Joensuun Maila", "Kempeleen Kiri", "Kouvolan Pallonlyojat", "Seinajoen Jimi Gissit", "Silinjarven Pesis", "Manse PP"]', 0, '2020-06-17 00:00:00')
ON CONFLICT (fk_bookmaker_id, fk_bookmaker_sport_id, title) DO UPDATE SET outcomes = EXCLUDED.outcomes;