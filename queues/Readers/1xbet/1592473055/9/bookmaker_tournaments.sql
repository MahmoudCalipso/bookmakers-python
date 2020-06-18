INSERT INTO bookmaker_tournaments (id, fk_bookmaker_id, fk_bookmaker_sport_id, title, skip, mapped, created_at, found_in) VALUES 
(DEFAULT, 14, {sport=Martial Arts}, 'Combatsport. KSW', 0, 0, '2020-06-18 00:00:00', 'Roman Szymanski vs Mateusz Legierski')
,(DEFAULT, 14, {sport=Football}, 'FIFA. eSports Battle. Night FA Cup', 0, 0, '2020-06-18 00:00:00', 'Everton (KRaftVK) vs Leicester City (Kray)')
ON CONFLICT (fk_bookmaker_id, fk_bookmaker_sport_id, title) DO NOTHING;