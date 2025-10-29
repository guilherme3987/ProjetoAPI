# extract.py tem DataConsolidator em ~/projects/ProjetoAPI/venv/App/Extract

import os
import pandas as pd
from .AuxFunc.auxfunc import FileProcessor



class DataConsolidator:
    def __init__(self, data_dir=None):
        if data_dir is None:
            self.file_processor = FileProcessor()  # Usa diretório padrão
        else:
            self.file_processor = FileProcessor(data_dir)
        
        self.DATA_DIR = self.file_processor.get_data_dir()

    def read_csv(self, file_path):
        try:
            df = pd.read_csv(file_path)
            print(f"CSV lido: {os.path.basename(file_path)} - {len(df)} linhas")
            return df
        except Exception as e:
            print(f" Erro ao ler CSV {file_path}: {e}")
            return pd.DataFrame()
    
    def read_xlsx(self, file_path):
        """
        Lê arquivo XLSX, verificando se tem múltiplas planilhas
        """
        try:
            # Verifica se tem múltiplas planilhas
            has_multiple_sheets = self.file_processor.check_xlsx(
                file_name=os.path.basename(file_path), 
                check_multiple_sheets=True, 
                file_path=file_path
            )
            
            if has_multiple_sheets:
                # Lê todas as planilhas do arquivo
                return self._read_multiple_sheets(file_path)
            else:
                # Lê apenas a primeira planilha
                df = pd.read_excel(file_path)
                print(f" XLSX lido: {os.path.basename(file_path)} - {len(df)} linhas")
                return df
                
        except Exception as e:
            print(f"Erro ao ler XLSX {file_path}: {e}")
            return pd.DataFrame()
    
    def _read_multiple_sheets(self, file_path):
        """
        Lê todas as planilhas de um arquivo XLSX com múltiplas abas
        """
        try:
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names
            all_sheets_df = pd.DataFrame()
            
            print(f"Lendo XLSX com múltiplas planilhas: {os.path.basename(file_path)}")
            
            for sheet_name in sheet_names:
                try:
                    # Lê cada planilha individualmente
                    df_sheet = pd.read_excel(file_path, sheet_name=sheet_name)
                    
                    # Adiciona uma coluna indicando a planilha de origem
                    if not df_sheet.empty:
                        df_sheet['_sheet_name'] = sheet_name
                        df_sheet['_file_name'] = os.path.basename(file_path)
                        all_sheets_df = pd.concat([all_sheets_df, df_sheet], ignore_index=True)
                        print(f" Planilha '{sheet_name}': {len(df_sheet)} linhas")
                    else:
                        print(f"Planilha '{sheet_name}': vazia")
                        
                except Exception as e:
                    print(f" Erro na planilha '{sheet_name}': {e}")
                    continue
            
            print(f"    Total consolidado do arquivo: {len(all_sheets_df)} linhas")
            return all_sheets_df
            
        except Exception as e:
            print(f"✗ Erro ao ler múltiplas planilhas do XLSX {file_path}: {e}")
            return pd.DataFrame()
    
    def consolidate_data(self):
        consolidate_df = pd.DataFrame()
        files = self.file_processor.acess_dir()

        if not files:
            print(" Nenhum arquivo encontrado no diretório.")
            return consolidate_df
        
        print(f"\n Processando {len(files)} arquivos em: {self.DATA_DIR}")
        
        for file in files:
            file_path = os.path.join(self.DATA_DIR, file)

            # Verificação básica de CSV
            if self.file_processor.check_csv(file):
                print(f"\n--- Processando CSV: {file} ---")
                df = self.read_csv(file_path)
                if not df.empty:
                    consolidate_df = pd.concat([consolidate_df, df], ignore_index=True)
                    print(f" CSV adicionado: {file}")
            
            # Verificação básica de XLSX
            elif self.file_processor.check_xlsx(file):  
                print(f"\n--- Processando XLSX: {file} ---")
                df = self.read_xlsx(file_path)  
                if not df.empty:
                    consolidate_df = pd.concat([consolidate_df, df], ignore_index=True)
                    print(f" XLSX adicionado: {file}")
            else:
                print(f"  Ignorando {file} - não é CSV nem XLSX")
        
        # Resumo final
        if not consolidate_df.empty:
            print(f"\n CONSOLIDAÇÃO CONCLUÍDA!")
            print(f" Total de linhas: {len(consolidate_df)}")
            print(f" Total de colunas: {len(consolidate_df.columns)}")
            
            # Verifica se há metadados de múltiplas planilhas
            metadata_cols = [col for col in consolidate_df.columns if col.startswith('_')]
            if metadata_cols:
                print(f"Metadados detectados: {metadata_cols}")
                if '_sheet_name' in consolidate_df.columns:
                    print(f"Distribuição por planilha:")
                    sheet_counts = consolidate_df['_sheet_name'].value_counts()
                    for sheet, count in sheet_counts.items():
                        print(f"   ├── {sheet}: {count} linhas")
        else:
            print(f"\n Nenhum dado foi consolidado")
        
        return consolidate_df

    def get_data_frame(self):
        return self.consolidate_data()

