package edu.utexas.clm.archipelago.ijsupport.bottle.tile;

import edu.utexas.clm.archipelago.network.MessageXC;
import edu.utexas.clm.archipelago.network.translation.Bottle;
import ini.trakem2.display.Patch;
import mpicbg.models.RigidModel2D;
import mpicbg.trakem2.align.AbstractAffineTile2D;
import mpicbg.trakem2.align.RigidTile2D;

import java.io.IOException;

/**
 *
 */
public class RigidTile2DBottle implements Bottle<AbstractAffineTile2D<?>>
{
    private final RigidModel2D model;
    private final Patch patch;

    public RigidTile2DBottle(final RigidTile2D tile)
    {
        model = tile.getModel();
        patch = tile.getPatch();
    }

    public AbstractAffineTile2D<?> unBottle(final MessageXC xc) throws IOException
    {
        return new RigidTile2D(model, patch);
    }
}
