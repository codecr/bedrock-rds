-- Crear una tabla para almacenar el historial de consultas y optimizaciones
CREATE TABLE IF NOT EXISTS query_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    original_query TEXT,
    optimized_query TEXT,
    execution_time FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Función para generar consultas SQL optimizadas usando Amazon Bedrock
CREATE FUNCTION invoke_sonnet (request_body TEXT)
    RETURNS TEXT
    ALIAS AWS_BEDROCK_INVOKE_MODEL
    MODEL ID 'anthropic.claude-3-5-sonnet-20240620-v1:0'
    CONTENT_TYPE 'application/json'
    ACCEPT 'application/json';
	

-- Funcion para invocar el modelo de Claude	
DELIMITER //
CREATE FUNCTION generate_optimized_query(input_query TEXT, schema_info TEXT) 
RETURNS TEXT
BEGIN
    DECLARE result TEXT;
    DECLARE prompt TEXT;
    DECLARE json_payload TEXT;
    
    SET prompt = CONCAT('Actúa como un experto en optimización de bases de datos MySQL. ',
                'Dada la siguiente consulta SQL y la información del esquema, ',
                'proporciona una versión optimizada de la consulta. ',
                'Solo devuelve la consulta optimizada, sin explicaciones. ',
                'Consulta original: "', input_query, '" ',
                'Información del esquema: "', schema_info, '"');
    
    SET json_payload = JSON_OBJECT(
        'anthropic_version', 'bedrock-2023-05-31',
        'max_tokens', 500,
        'messages', JSON_ARRAY(
            JSON_OBJECT(
                'role', 'user',
                'content', JSON_ARRAY(
                    JSON_OBJECT(
                        'type', 'text',
                        'text', prompt
                    )
                )
            )
        )
    );
    
    SET result = invoke_sonnet(json_payload);
    
    RETURN JSON_UNQUOTE(JSON_EXTRACT(result, '$.content[0].text'));
END //
DELIMITER ;



-- Procedimiento para analizar y optimizar una consulta
DELIMITER //
CREATE PROCEDURE analyze_and_optimize_query(IN input_query TEXT)
BEGIN
    DECLARE optimized_query TEXT;
    DECLARE original_execution_time FLOAT;
    DECLARE optimized_execution_time FLOAT;
    DECLARE schema_info TEXT;
    
    -- Obtener información del esquema
    SET schema_info = (
        SELECT GROUP_CONCAT(CONCAT(table_name, ': ', column_names) SEPARATOR '; ')
        FROM (
            SELECT 
                table_name, 
                GROUP_CONCAT(column_name ORDER BY ordinal_position) AS column_names
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
            GROUP BY table_name
        ) AS schema_data
    );
    
    -- Generar consulta optimizada
    SET optimized_query = generate_optimized_query(input_query, schema_info);
    
    -- Medir tiempo de ejecución de la consulta original
    SET @start_time = NOW(6);
    SET @sql = input_query;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET original_execution_time = TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(6)) / 1000000;
    
    -- Medir tiempo de ejecución de la consulta optimizada
    SET @start_time = NOW(6);
    SET @sql = optimized_query;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET optimized_execution_time = TIMESTAMPDIFF(MICROSECOND, @start_time, NOW(6)) / 1000000;
    
    -- Guardar resultados en el historial
    INSERT INTO query_history (original_query, optimized_query, execution_time)
    VALUES (input_query, optimized_query, optimized_execution_time);
    
    -- Mostrar resultados
    SELECT 
        'Original Query' AS query_type, 
        input_query AS query, 
        original_execution_time AS execution_time
    UNION ALL
    SELECT 
        'Optimized Query' AS query_type, 
        optimized_query AS query, 
        optimized_execution_time AS execution_time;
END //
DELIMITER ;

-- Ejemplo de uso
CALL analyze_and_optimize_query('
    SELECT c.first_name, c.last_name, 
           COUNT(r.rental_id) as rental_count, 
           SUM(p.amount) as total_spent
    FROM customer c
    JOIN rental r ON c.customer_id = r.customer_id
    JOIN payment p ON r.rental_id = p.rental_id
    JOIN inventory i ON r.inventory_id = i.inventory_id
    JOIN film f ON i.film_id = f.film_id
    WHERE f.rating = "PG" AND YEAR(r.rental_date) = 2005
    GROUP BY c.customer_id
    HAVING rental_count > 5
    ORDER BY total_spent DESC
    LIMIT 10
');
