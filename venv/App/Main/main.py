# main.py em ~/projects/ProjetoAPI/venv/App/Main

import os
import sys
# Adiciona o diretório pai ao sys.path para importar Extract
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from Extract.extract import DataConsolidator

def main():

    consolidator = DataConsolidator()
    consolidated_data = consolidator.get_data_frame()
    
    print(f"\n" + "="*50)
    print("RESUMO FINAL")
    print("="*50)
    print(f"Shape do DataFrame: {consolidated_data.shape}")
    
    if not consolidated_data.empty:
        print(consolidated_data.head())
        print(f"\nColunas disponíveis: {list(consolidated_data.columns)}")
    else:
        print("Nenhum dado para mostrar.")


if __name__ == "__main__":
    main()