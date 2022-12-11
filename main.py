from Scripts.preprocessing import ModelTraining

if __name__=='__main__':
    model = ModelTraining()
    model.read_data(model.csv_PATH,'spark')
    # model.cleaning('train')
    # model.Pipeline()
    model.evaluation()