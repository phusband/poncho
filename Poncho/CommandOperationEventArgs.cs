using System;

namespace Poncho
{
    public sealed class CommandOperationEventArgs : EventArgs
    {
        private readonly CommandOperation _operation;
        private readonly ObservableDbCommand _command;
        private readonly object _commandData;

        public object CommandData => _commandData;
        public CommandOperation Operation => _operation;
        public ObservableDbCommand Command => _command;

        public CommandOperationEventArgs(ObservableDbCommand command, object data, CommandOperation operation = CommandOperation.None)
        {
            _operation = operation;
            _commandData = data;
            _command = command;
        }
    }
}
