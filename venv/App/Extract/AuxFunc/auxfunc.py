# auxfunc.py tem FileProcessor em ~/projects/ProjetoAPI/venv/App/Extract/AuxFunc

import os
import pandas as pd

"""
Classe para processar arquivos em um diretório específico
Métodos para acessar diretório, verificar tipos de arquivos e analisar planilhas XLSX
"""

class FileProcessor:
    def __init__(self, data_dir='/home/guilherme/projects/ProjetoAPI/venv/Data'):
        self.DATA_DIR = data_dir

    def acess_dir(self):
        try:
            content = os.listdir(self.DATA_DIR)
            return content
        except FileNotFoundError:
            print(f"Directory {self.DATA_DIR} not found.")
            return []
    
    def check_csv(self, file_name):
        return file_name.lower().endswith('.csv')
    
    def check_xlsx(self, file_name, check_multiple_sheets=False, file_path=None):
        """
        Verifica se é arquivo XLSX e  se contém múltiplas planilhas
        """
        # Primeiro verifica se é arquivo XLSX
        is_xlsx = file_name.lower().endswith('.xlsx')
        
        # Se não for verificar múltiplas planilhas ou não for XLSX, retorna simples
        if not check_multiple_sheets or not is_xlsx:
            return is_xlsx
        
        # Se for verificar múltiplas planilhas, precisa do file_path
        if file_path is None:
            file_path = os.path.join(self.DATA_DIR, file_name)
        
        return self._check_multiple_sheets(file_path)

    def _check_multiple_sheets(self, file_path):
        """
        Verifica se um arquivo XLSX contém múltiplas planilhas
        """
        try:
            excel_file = pd.ExcelFile(file_path)
            sheet_count = len(excel_file.sheet_names)
            
            print(f" Analisando arquivo: {os.path.basename(file_path)}")
            print(f"  Número de planilhas: {sheet_count}")
            print(f"  Planilhas: {excel_file.sheet_names}")
            
            if sheet_count > 1:
                print(f"x MULTIPLAS PLANILHAS DETECTADAS")
                return True
            else:
                print(f" Apenas uma planilha")
                return False
                
        except Exception as e:
            print(f"    Erro ao analisar arquivo XLSX: {e}")
            return None

    def get_xlsx_sheet_info(self, file_path):
        """
        Obtém informações detalhadas sobre as planilhas de um arquivo XLSX
        Retorna um dicionário com informações ou None em caso de erro
        """
        try:
            excel_file = pd.ExcelFile(file_path)
            sheet_info = {
                'file_name': os.path.basename(file_path),
                'sheet_count': len(excel_file.sheet_names),
                'sheet_names': excel_file.sheet_names,
                'sheets_details': []
            }
            
            # Obtém informações de cada planilha
            for sheet_name in excel_file.sheet_names:
                try:
                    df_temp = pd.read_excel(file_path, sheet_name=sheet_name, nrows=1)
                    sheet_detail = {
                        'name': sheet_name,
                        'columns': list(df_temp.columns) if not df_temp.empty else [],
                        'empty': df_temp.empty
                    }
                    sheet_info['sheets_details'].append(sheet_detail)
                except Exception as e:
                    sheet_info['sheets_details'].append({
                        'name': sheet_name,
                        'columns': [],
                        'empty': True,
                        'error': str(e)
                    })
            
            return sheet_info
            
        except Exception as e:
            print(f" Erro ao obter informações das planilhas: {e}")
            return None

    def get_data_dir(self):
        return self.DATA_DIR
    
    def set_data_dir(self, new_path):
        self.DATA_DIR = new_path