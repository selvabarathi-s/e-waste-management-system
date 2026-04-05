const simulationService = require('../services/simulationService');
const { AppError } = require('../middleware/errorHandler');

exports.runSimulation = async (req, res, next) => {
  try {
    const result = await simulationService.runSimulation(req.body);
    res.status(201).json(result);
  } catch (e) {
    next(e);
  }
};

exports.getScenarios = async (req, res, next) => {
  try {
    const scenarios = await simulationService.getScenarios();
    res.status(200).json(scenarios);
  } catch (e) {
    next(e);
  }
};

exports.deleteScenario = async (req, res, next) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (Number.isNaN(id)) throw new AppError('Invalid scenario ID', 400, 'VALIDATION_ERROR');
    const result = await simulationService.deleteScenario(id);
    res.status(200).json(result);
  } catch (e) {
    next(e);
  }
};

exports.compareScenarios = async (req, res, next) => {
  try {
    const { ids } = req.body;
    if (!ids || !Array.isArray(ids)) {
      throw new AppError('Request body must contain an "ids" array', 400, 'VALIDATION_ERROR');
    }
    const numericIds = ids.map((id) => parseInt(id, 10)).filter((id) => !Number.isNaN(id));
    if (numericIds.length < 2) {
      throw new AppError('At least 2 valid scenario IDs are required', 400, 'VALIDATION_ERROR');
    }
    const result = await simulationService.compareScenarios(numericIds);
    res.status(200).json(result);
  } catch (e) {
    next(e);
  }
};
