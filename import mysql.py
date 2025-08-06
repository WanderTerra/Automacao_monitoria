import mysql.connector

# Import the functions to test
from transcrever_audios import (
    get_db_connection,
    carregar_mapeamento_call_ids,
    extrair_call_id_original,
    map_resultado_value,
    extrair_descricao_e_peso,
    salvar_avaliacao_no_banco,
    extrair_agent_id,
    corrigir_portes_advogados,
    corrigir_vuon_card,
    corrigir_aguas_guariroba,
    corrigir_assessoria_juridica,
    parse_vtt,
    classificar_falantes_com_gpt,
    process_audio_file,
    avaliar_ligacao,
    redistribuir_pesos_e_pontuacao,
    calcular_duracao_audio_robusto,
    format_time_now,
    CarteiraConfig,
    ProcessadorCarteira,
    PROMPTS_AVALIACAO,
    DB_CONFIG
)


class TestDatabaseFunctions:
    """Test database-related functions"""
    
    @patch('transcrever_audios.mysql.connector.connect')
    def test_get_db_connection_success(self, mock_connect):
        """Test successful database connection"""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        result = get_db_connection()
        
        assert result == mock_conn
        mock_connect.assert_called_once_with(**DB_CONFIG)
    
    @patch('transcrever_audios.mysql.connector.connect')
    def test_get_db_connection_failure(self, mock_connect):
        """Test database connection failure"""
        mock_connect.side_effect = mysql.connector.Error("Connection failed")
        
        with pytest.raises(mysql.connector.Error):
            get_db_connection()
    
    @patch('builtins.open', new_callable=mock_open, read_data='nome_arquivo,call_id\ntest.mp3,12345\ntest2.mp3,67890')
    @patch('transcrever_audios.csv.DictReader')
    def test_carregar_mapeamento_call_ids_success(self, mock_dict_reader, mock_file):
        """Test successful loading of call ID mapping"""
        mock_dict_reader.return_value = [
            {'nome_arquivo': 'test.mp3', 'call_id': '12345'},
            {'nome_arquivo': 'test2.mp3', 'call_id': '67890'}
        ]
        
        result = carregar_mapeamento_call_ids('/test/path')
        
        expected = {'test.mp3': '12345', 'test2.mp3': '67890'}
        assert result == expected
    
    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_carregar_mapeamento_call_ids_file_not_found(self, mock_file):
        """Test handling of missing mapping file"""
        result = carregar_mapeamento_call_ids('/test/path')
        
        assert result == {}
    
    def test_map_resultado_value(self):
        """Test status mapping function"""
        assert map_resultado_value('C') == 'CONFORME'
        assert map_resultado_value('NC') == 'NAO CONFORME'
        assert map_resultado_value('NA') == 'NAO SE APLICA'
        assert map_resultado_value('N/A') == 'NAO SE APLICA'
        assert map_resultado_value('N\\A') == 'NAO SE APLICA'